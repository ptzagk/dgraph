/*
 * Copyright 2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package backup

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
)

type backup struct {
	ticker   *time.Ticker
	wg       sync.WaitGroup
	tempFile string    // staging file
	location string    // URL to final destination
	aborts   uint64    // Num of aborts
	start    time.Time // To get time elapsel.
	zc       intern.ZeroClient
}

func (b *backup) process() {
	defer b.wg.Done()
	x.Printf("Backup: Processing %+v\n", b)
	req := &intern.BackupRequest{
		Destination: b.tempFile,
	}
	res, err := b.zc.Backup(context.Background(), req)
	if err != nil {
		x.Println("process:", err)
		return
	}
	x.Printf("%+v", res)
}
