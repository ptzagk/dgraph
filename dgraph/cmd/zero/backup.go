/*
 * Copyright 2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package zero

import (
	"hash/crc32"
	"io"
	"sync"

	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
)

func (s *Server) Backup(req *intern.BackupRequest, stream intern.Zero_BackupServer) error {
	var wg sync.WaitGroup
	ckvs, cerr := make(chan *intern.KVS, 100), make(chan error, 1)
	wg.Add(1)
	go processKVS(&wg, stream, ckvs, cerr)

	for _, group := range s.KnownGroups() {
		pl := s.Leader(group)
		if pl == nil {
			x.Printf("Backup: No healthy connection found to leader of group %d\n", group)
			continue
		}

		x.Printf("Backup: Requesting snapshot: group %d\n", group)
		ctx, worker := s.Node.ctx, intern.NewWorkerClient(pl.Get())
		kvs, err := worker.StreamSnapshot(ctx, &intern.Snapshot{})
		if err != nil {
			return err
		}

		count := 0
		for kvs := range fetchKVS(kvs, cerr) {
			select {
			case ckvs <- kvs:
				count += len(kvs.Kv)
			case <-ctx.Done():
				close(ckvs)
				return ctx.Err()
			case err := <-cerr:
				x.Println("Failure:", err)
				close(ckvs)
				return err
			}
		}
		x.Printf("Backup: Group %d sent %d keys.\n", group, count)
	}

	close(ckvs)
	wg.Wait()

	// check for any errors from processKVS
	if err := <-cerr; err != nil {
		x.Println("Error:", err)
		return err
	}

	return nil
}

// fetchKVS gets streamed snapshot from worker.
func fetchKVS(stream intern.Worker_StreamSnapshotClient, cerr chan error) chan *intern.KVS {
	out := make(chan *intern.KVS)
	go func() {
		for {
			kvs, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cerr <- err
				break
			}
			out <- kvs
		}
		close(out)
	}()
	return out
}

// processKVS unrolls the KVS list values and streams them back to the client.
// Postprocessing should happen at the client side.
func processKVS(wg *sync.WaitGroup, stream intern.Zero_BackupServer, in chan *intern.KVS,
	cerr chan error) {
	defer wg.Done()

	h := crc32.NewIEEE()
	for kvs := range in {
		for _, kv := range kvs.Kv {
			if kv.Version == 0 {
				continue
			}
			b, err := kv.Marshal()
			if err != nil {
				cerr <- err
				return
			}

			resp := &intern.BackupResponse{
				Data:     b,
				Length:   uint64(len(b)),
				Checksum: h.Sum(b),
			}
			if err := stream.Send(resp); err != nil {
				cerr <- err
				return
			}
		}
	}
	cerr <- nil
}
