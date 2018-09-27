/*
 * Copyright 2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package backup

import (
	"bytes"
	"context"
	"crypto/md5"
	"io"
	"io/ioutil"
	"time"

	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"google.golang.org/grpc"
)

type backup struct {
	h        handler
	dst      string
	start    time.Time // To get time elapsel.
	zeroconn *grpc.ClientConn
}

func (b *backup) process() error {
	tempFile, err := ioutil.TempFile(opt.tmpDir, "dgraph-backup-*")
	if err != nil {
		return err
	}
	defer tempFile.Close()

	zc := intern.NewZeroClient(b.zeroconn)
	stream, err := zc.Backup(context.Background(), &intern.BackupRequest{})
	if err != nil {
		return err
	}

	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		csum0 := md5.Sum(res.Data)
		if !bytes.Equal(csum0[:], res.Checksum) {
			x.Printf("Warning: data checksum failed: csum0 != %x\n", res.Checksum)
			continue
		}

		if _, err := pbutil.WriteDelimited(tempFile, res); err != nil {
			return x.Errorf("Backup: could not save to temp file: %s\n", err)
		}
	}

	if err := b.h.Copy(tempFile.Name(), b.dst); err != nil {
		return x.Errorf("Backup: could not copy to destination: %s\n", err)
	}

	return nil
}
