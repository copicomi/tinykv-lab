package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := &kvrpcpb.RawGetResponse{}
	reader, _ := server.storage.Reader(nil)
	defer reader.Close()
	val, _ := reader.GetCF(req.Cf, req.Key)
	if val == nil {
		response.NotFound = true
	}
	response.Value = val
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	server.storage.Write(nil, batch)
	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	server.storage.Write(nil, batch)
	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := &kvrpcpb.RawScanResponse{}
	reader, _ := server.storage.Reader(nil)
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	pairs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, _ := item.ValueCopy(nil)
		pair := &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		pairs = append(pairs, pair)
		if len(pairs) >= int(req.Limit) {
			break
		}
	}
	response.Kvs = pairs
	return response, nil
}
