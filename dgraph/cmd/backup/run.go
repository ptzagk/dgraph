/*
 * Copyright 2017-2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package backup

import (
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
	"github.com/spf13/cobra"
)

type options struct {
	// dgraph              string
	zero string
	// concurrent          int
	clientDir string
}

var opt options
var tlsConf x.TLSHelperConfig

var Backup x.SubCommand

func init() {
	Backup.Cmd = &cobra.Command{
		Use:   "backup",
		Short: "Run Dgraph backup",
		Run: func(cmd *cobra.Command, args []string) {
			defer x.StartProfile(Backup.Conf).Stop()
			run()
		},
	}
	Backup.EnvPrefix = "DGRAPH_BACKUP"

	flag := Backup.Cmd.Flags()
	flag.StringP("zero", "z", "127.0.0.1:5080", "Dgraphzero gRPC server address")
	flag.IntP("conc", "c", 100,
		"Number of concurrent requests to make to Dgraph")

	// TLS configuration
	x.RegisterTLSFlags(flag)
	flag.Bool("tls_insecure", false, "Skip certificate validation (insecure)")
	flag.String("tls_ca_certs", "", "CA Certs file path.")
	flag.String("tls_server_name", "", "Server name.")
}

func setupConnection(host string, insecure bool) (*grpc.ClientConn, error) {
	if insecure {
		return grpc.Dial(host,
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(x.GrpcMaxSize),
				grpc.MaxCallSendMsgSize(x.GrpcMaxSize)),
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithTimeout(10*time.Second))
	}

	tlsConf.ConfigType = x.TLSClientConfig
	tlsConf.CertRequired = false
	tlsCfg, _, err := x.GenerateTLSConfig(tlsConf)
	if err != nil {
		return nil, err
	}

	return grpc.Dial(host,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(x.GrpcMaxSize),
			grpc.MaxCallSendMsgSize(x.GrpcMaxSize)),
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithBlock(),
		grpc.WithTimeout(10*time.Second))
}

func fileList(files string) []string {
	if len(files) == 0 {
		return []string{}
	}
	return strings.Split(files, ",")
}

func setup() *backup {
	zero, err := setupConnection(opt.zero, true)
	x.Checkf(err, "Unable to connect to zero, Is it running at %s?", opt.zero)

	b := &backup{
		start: time.Now(),
		zc:    intern.NewZeroClient(zero),
	}

	return b
}

func run() {
	opt = options{
		zero: Backup.Conf.GetString("zero"),
		// concurrent:          Backup.Conf.GetInt("conc"),
	}
	x.LoadTLSConfig(&tlsConf, Backup.Conf)
	tlsConf.Insecure = Backup.Conf.GetBool("tls_insecure")
	tlsConf.RootCACerts = Backup.Conf.GetString("tls_ca_certs")
	tlsConf.ServerName = Backup.Conf.GetString("tls_server_name")

	go http.ListenAndServe("localhost:6060", nil)

	b := setup()

	b.wg.Add(1)
	go b.process()
	b.wg.Wait()
}
