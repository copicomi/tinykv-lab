package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
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
	response := &kvrpcpb.GetResponse{}
	keys := [][]byte{req.Key}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := mvccTxn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= req.GetVersion() {
		response.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return response, nil
	}
	value, err := mvccTxn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		response.NotFound = true
	} else {
		response.Value = value
	}
	return response, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	muts := req.Mutations
	keys := make([][]byte, 0, len(muts))
	for _, mut := range muts {
		keys = append(keys, mut.Key)
	}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	response := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	for _, mut := range muts {
		write, commitTs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			return nil, err
		}
		if write != nil && commitTs >= req.GetStartVersion() {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    write.StartTS,
					ConflictTs: commitTs,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		existingLock, err := txn.GetLock(mut.Key)
		if err != nil {
			return nil, err
		}
		if existingLock != nil && existingLock.Ts != req.GetStartVersion() {
			response.Errors = append(response.Errors, &kvrpcpb.KeyError{
				Locked: existingLock.Info(mut.Key),
			})
			continue
		}
		lock := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.GetStartVersion(),
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(mut.Op),
		}
		txn.PutLock(mut.Key, lock)
		if mut.Op == kvrpcpb.Op_Put {
			txn.PutValue(mut.Key, mut.Value)
		} else if mut.Op == kvrpcpb.Op_Del {
			txn.DeleteValue(mut.Key)
		}
	}

	if len(response.Errors) == 0 {
		server.storage.Write(req.Context, txn.Writes())
	}

	return response, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	keys := req.Keys
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	response := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}

	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts != req.GetStartVersion() {
			response.Error = &kvrpcpb.KeyError{
				Locked:    lock.Info(key),
				Retryable: "true",
			}
			return response, nil
		}
		if lock == nil {
			write, _, err := txn.MostRecentWrite(key)
			if err != nil {
				return nil, err
			}
			if write != nil {
				if write.Kind == mvcc.WriteKindRollback {
					response.Error = &kvrpcpb.KeyError{
						Retryable: "false",
					}
					return response, nil
				}
			}
			continue
		}
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return response, nil

}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()

	limit := req.GetLimit()
	for i := uint32(0); i < limit; {
		key, val, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts <= req.GetVersion() {
			response.Pairs = append(response.Pairs, &kvrpcpb.KvPair{
				Key: key,
				Error: &kvrpcpb.KeyError{
					Locked: lock.Info(key),
				},
			})
			i++
			continue
		}
		if val != nil {
			response.Pairs = append(response.Pairs, &kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			})
			i++
		}
	}
	return response, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.CheckTxnStatusResponse{}
	server.Latches.AcquireLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	write, commitTS, err := txn.MostRecentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			response.CommitVersion = commitTS
		}
		return response, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		txn.PutWrite(req.PrimaryKey, req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		response.Action = kvrpcpb.Action_LockNotExistRollback
		return response, nil
	}

	lockTTL := lock.Ttl
	currentPhysical := mvcc.PhysicalTime(req.CurrentTs)
	startPhysical := mvcc.PhysicalTime(req.GetLockTs())
	if currentPhysical > startPhysical+uint64(lockTTL) {
		txn.PutWrite(req.PrimaryKey, req.GetLockTs(), &mvcc.Write{
			StartTS: req.GetLockTs(),
			Kind:    mvcc.WriteKindRollback,
		})
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}
		response.Action = kvrpcpb.Action_TTLExpireRollback
		return response, nil
	}
	response.LockTtl = lockTTL
	return response, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.BatchRollbackResponse{}
	keys := req.Keys
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				response.Error = &kvrpcpb.KeyError{
					Retryable: "false",
				}
				return response, nil
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			if lock.Ts != req.GetStartVersion() {
				txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
					Kind:    mvcc.WriteKindRollback,
					StartTS: req.GetStartVersion(),
				})
				continue
			}
			txn.DeleteLock(key)
			txn.DeleteValue(key)
		}
		txn.PutWrite(key, req.GetStartVersion(), &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: req.GetStartVersion(),
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	response := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			response.RegionError = regionErr.RequestErr
			return response, nil
		}
		return nil, err
	}
	defer reader.Close()

	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	var keys [][]byte
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		lock, err := mvcc.ParseLock(val)
		if err != nil {
			return nil, err
		}
		if lock.Ts == req.GetStartVersion() {
			keys = append(keys, item.Key())
		}
	}

	if len(keys) == 0 {
		return response, nil
	}

	if req.GetCommitVersion() == 0 {
		batchRollbackReq := &kvrpcpb.BatchRollbackRequest{
			Keys:         keys,
			StartVersion: req.GetStartVersion(),
			Context:      req.Context,
		}
		rbResp, err := server.KvBatchRollback(context.Background(), batchRollbackReq)
		if err != nil {
			return nil, err
		}
		response.RegionError, response.Error = rbResp.RegionError, rbResp.Error
	} else {
		commitReq := &kvrpcpb.CommitRequest{
			Keys:          keys,
			StartVersion:  req.GetStartVersion(),
			CommitVersion: req.GetCommitVersion(),
			Context:       req.Context,
		}
		cResp, err := server.KvCommit(context.Background(), commitReq)
		if err != nil {
			return nil, err
		}
		response.RegionError, response.Error = cResp.RegionError, cResp.Error
	}

	return response, nil
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
