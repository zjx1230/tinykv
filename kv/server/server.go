package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pkg/errors"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	storageReader, err := server.storage.Reader(req.Context)
	defer storageReader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	txn := mvcc.NewMvccTxn(storageReader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}

	if lock != nil && lock.Ts <= txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	allKeys := make([][]byte, 0)
	for _, mutation := range req.Mutations {
		allKeys = append(allKeys, mutation.Key)
	}

	server.Latches.WaitForLatches(allKeys)
	defer server.Latches.ReleaseLatches(allKeys)

	storageReader, err := server.storage.Reader(req.Context)
	defer storageReader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	keyErrors := make([]*kvrpcpb.KeyError, 0)
	for _, mutation := range req.Mutations {
		write, commitTs, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if write != nil && commitTs >= txn.StartTS {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: commitTs,
					Key:        mutation.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}

		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if lock != nil && lock.Ts <= txn.StartTS {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mutation.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}

		var writeKind mvcc.WriteKind
		switch mutation.Op {
		case kvrpcpb.Op_Put:
			writeKind = mvcc.WriteKindPut
			txn.PutValue(mutation.Key, mutation.Value)
		case kvrpcpb.Op_Del:
			writeKind = mvcc.WriteKindDelete
			txn.DeleteValue(mutation.Key)
		default:
			return nil, errors.New("Invalid op.")
		}

		locks := mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    writeKind,
		}
		txn.PutLock(mutation.Key, &locks)
	}

	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
		return resp, nil
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	storageReader, err := server.storage.Reader(req.Context)
	defer storageReader.Close()
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}

		if lock == nil { // no lock maybe another transaction rollback
			write, commitTs, err := txn.MostRecentWrite(key)
			if err != nil {
				if regionErr, ok := err.(*raft_storage.RegionError); ok {
					resp.RegionError = regionErr.RequestErr
					return resp, nil
				}
				return nil, err
			}

			if write.Kind == mvcc.WriteKindRollback && commitTs <= req.CommitVersion {
				resp.Error = &kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    txn.StartTS,
						ConflictTs: commitTs,
						Key:        key,
					},
				}
			}
			return resp, nil
		}

		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
