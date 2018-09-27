/*
 * Copyright 2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package zero

import (
	"crypto/md5"
	"io"
	"sync"

	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
)

func (s *Server) Backup(req *intern.BackupRequest, stream intern.Zero_BackupServer) error {
	ctx := s.Node.ctx
	cerr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	ckvs := make(chan *intern.KVS, 100)
	go processKVS(&wg, stream, ckvs, cerr)

	for _, group := range s.KnownGroups() {
		pl := s.Leader(group)
		if pl == nil {
			x.Printf("Backup: No healthy connection found to leader of group %d\n", group)
			continue
		}

		x.Printf("Backup: Requesting snapshot: group %d\n", group)
		worker := intern.NewWorkerClient(pl.Get())
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
func processKVS(
	wg *sync.WaitGroup,
	stream intern.Zero_BackupServer,
	in chan *intern.KVS,
	cerr chan error,
) {
	defer wg.Done()
	var csum [16]byte
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
			csum = md5.Sum(b)
			resp := &intern.BackupResponse{
				Data:     b,
				Length:   uint64(len(b)),
				Checksum: csum[:],
			}
			if err := stream.Send(resp); err != nil {
				cerr <- err
				return
			}
		}
	}
	cerr <- nil
}
