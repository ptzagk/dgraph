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

type collector struct {
	kv []*intern.KV
}

func (c *collector) Send(kvs *intern.KVS) error {
	c.kv = append(c.kv, kvs.Kv...)
	return nil
}

type backup struct {
	ticker *time.Ticker
	wg     sync.WaitGroup

	// Num of aborts
	aborts uint64
	// To get time elapsel.
	start time.Time

	zc intern.ZeroClient
}

func (b *backup) process() {
	defer b.wg.Done()
	x.Printf("Backup: Processing %+v\n", b)
	req := &intern.BackupRequest{
		Destination: "somewhere",
	}
	stream, err := b.zc.Backup(context.Background(), req)
	if err != nil {
		x.Println("process:", err)
		return
	}
	x.Printf("%+v", stream)
}
