/*
 * Copyright 2018 Dgraph Labs, Inc.
 *
 * This file is available under the Apache License, Version 2.0,
 * with the Commons Clause restriction.
 */

package zero

import (
	"github.com/dgraph-io/dgraph/protos/intern"
	"github.com/dgraph-io/dgraph/x"
	"golang.org/x/net/context"
)

func (s *Server) Backup(ctx context.Context, req *intern.BackupRequest) (*intern.BackupResponse, error) {
	x.Printf("SERVER BACKUP: %+v\n", req)
	var res intern.BackupResponse
	if !s.Node.AmLeader() {
		res.Status = intern.BackupResponse_FAILED
		res.Message = errNotLeader.Error()
		return &res, errNotLeader
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
		kvs, err := worker.StreamSnapshot(context.Background(), &intern.Snapshot{})
		if err != nil {
			res.Status = intern.BackupResponse_FAILED
			res.Message = err.Error()
			break
			// continue
		}
		x.Printf("%+v", kvs)
		for i := range kvs.Recv() {
		}
		break
	}

	return &res, nil
}
