/*
 * Copyright 2017-2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package backup

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/dgraph-io/dgraph/x"
	"github.com/spf13/cobra"
)

type options struct {
	zero   string
	tmpDir string
	dstURL string
	full   bool
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
			if err := run(); err != nil {
				x.Printf("Backup: %s\n", err)
				os.Exit(1)
			}
		},
	}
	Backup.EnvPrefix = "DGRAPH_BACKUP"

	flag := Backup.Cmd.Flags()
	flag.StringP("zero", "z", "127.0.0.1:5080", "Dgraphzero gRPC server address")
	flag.StringP("tmpdir", "t", "", "Directory to store temporary files")
	flag.StringP("dst", "d", "", "URL path destination to store the backup file(s)")
	flag.Bool("full", true, "Full backup, otherwise incremental using dst value")
	// TLS configuration
	// x.RegisterTLSFlags(flag)
	Backup.Cmd.MarkFlagRequired("dst")
}

func run() error {
	opt = options{
		zero:   Backup.Conf.GetString("zero"),
		tmpDir: Backup.Conf.GetString("tmpdir"),
		dstURL: Backup.Conf.GetString("dst"),
		full:   Backup.Conf.GetBool("full"),
	}
	// x.LoadTLSConfig(&tlsConf, Backup.Conf)

	go http.ListenAndServe("localhost:6060", nil)

	b, err := setup()
	if err != nil {
		return err
	}
	fmt.Printf("Backup: saving to: %s ...\n", b.dst)
	if err := b.process(); err != nil {
		return err
	}
	fmt.Printf("Backup: time lapsed: %s\n", time.Since(b.start))

	return nil
}

func setup() (*backup, error) {
	// find handler
	h, err := findHandler(opt.dstURL)
	if err != nil {
		return nil, x.Errorf("Backup: failed to parse destination URL: %s", err)
	}

	// connect to zero
	connzero, err := setupConnection(opt.zero, true)
	if err != nil {
		return nil, x.Errorf("Backup: unable to connect to zero at %q: %s", opt.zero, err)
	}

	start := time.Now()
	b := &backup{
		start:    start,
		zeroconn: connzero,
		h:        h,
		dst:      dgraphBackupFullPrefix + start.Format(time.RFC3339) + dgraphBackupSuffix,
	}

	return b, nil
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
