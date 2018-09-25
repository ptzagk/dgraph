/*
 * Copyright 2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package zero

import (
	"io"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
	humanize "github.com/dustin/go-humanize"
	"golang.org/x/net/context"
)

func (s *Server) Backup(req *intern.BackupRequest, stream intern.Zero_BackupServer) error {
	x.Printf("SERVER BACKUP: %+v\n", req)
	if !s.Node.AmLeader() {
		return errNotLeader
	}

	groups := s.KnownGroups()
	for _, group := range groups {
		pl := s.Leader(group)
		if pl == nil {
			x.Printf("No healthy connection found to leader of group %d\n", group)
			continue
		}
		worker := intern.NewWorkerClient(pl.Get())
		x.Printf("Backup: Requesting snapshot: group %d\n", group)
		stream, err := worker.StreamSnapshot(context.Background(), &intern.Snapshot{})
		if err != nil {
			x.Println("Snapshot failed:", err)
			return err
		}

		ctx := context.Background()

		kvChan := make(chan *intern.KVS, 100)
		che := make(chan error, 1)
		go writeValue(ctx, kvChan, che)

		// We can use count to check the number of posting lists returned in tests.
		count := 0
		for {
			kvs, err := stream.Recv()
			if err == io.EOF {
				x.Printf("EOF has been reached\n")
				break
			}
			if err != nil {
				close(kvChan)
				return err
			}
			// We check for errors, if there are no errors we send value to channel.
			select {
			case kvChan <- kvs:
				count += len(kvs.Kv)
				// OK
			case <-ctx.Done():
				close(kvChan)
				return ctx.Err()
			case err := <-che:
				close(kvChan)
				// Important: Don't put return count, err
				// There was a compiler bug which was fixed in 1.8.1
				// https://github.com/golang/go/issues/21722.
				// Probably should be ok to return count, err now
				return err
			}
		}
		close(kvChan)

		if err := <-che; err != nil {
			return err
		}
		x.Printf("Group %d: Got %d keys. DONE.\n", group, count)
	}

	return nil
}

func writeValue(ctx context.Context, kvChan chan *intern.KVS, che chan error) {
	var bytesWritten uint64
	t := time.NewTicker(time.Second)
	defer t.Stop()
	go func() {
		now := time.Now()
		for range t.C {
			dur := time.Since(now)
			durSec := uint64(dur.Seconds())
			if durSec == 0 {
				continue
			}
			speed := bytesWritten / durSec
			x.Printf("Getting SNAPSHOT: Time elapsed: %v, bytes written: %s, %s/s\n",
				x.FixedDuration(dur), humanize.Bytes(bytesWritten), humanize.Bytes(speed))
		}
	}()

	var hasError int32
OUTER:
	for kvs := range kvChan {
		for _, kv := range kvs.Kv {
			if kv.Version == 0 {
				// Ignore this one. Otherwise, we'll get ErrManagedDB back, because every Commit in
				// managed DB must have a valid commit ts.
				continue
			}
			// Encountered an issue, no need to process the kv.
			if atomic.LoadInt32(&hasError) > 0 {
				break OUTER
			}

			// x.Printf("key=%s value=%s meta=%s\n", kv.Key, kv.Val, kv.UserMeta)
		}
	}

	if atomic.LoadInt32(&hasError) == 0 {
		che <- nil
	} else {
		che <- x.Errorf("Error while writing to badger")
	}
}
