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
	storageReader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	value, err := storageReader.GetCF(req.Cf, req.Key)
	if value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}

	return &kvrpcpb.RawGetResponse{Value: value, NotFound: false}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	op := make([]storage.Modify, 0)
	op = append(op, storage.Modify{
		Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf},
	})
	err := server.storage.Write(nil, op)
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	op := make([]storage.Modify, 0)
	op = append(op, storage.Modify{
		Data: storage.Delete{Key: req.Key, Cf: req.Cf},
	})
	err := server.storage.Write(nil, op)
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	storageReader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	iter := storageReader.IterCF(req.Cf)
	defer iter.Close()
	kvs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(req.StartKey); iter.Valid() && len(kvs) < int(req.Limit); iter.Next() {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: item.Key(), Value: value})
	}

	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
