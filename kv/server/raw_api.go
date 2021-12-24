package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	res :=  new(kvrpcpb.RawGetResponse)
	if server.storage == nil {
		res.Error = "storage not initialized"
		return res, errors.New("storage not initialized")
	}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil && err.Error() != "Key not found" {
		res.Error = err.Error()
		return res, err
	}

	res.Value = value
	res.NotFound = false
	if res.Value == nil {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res :=  new(kvrpcpb.RawPutResponse)

	if server.storage == nil {
		res.Error = "storage not initialized"
		return res, errors.New("storage not initialized")
	}

	var entries []storage.Modify
	entries = append(entries, storage.Modify{Data: storage.Put{
		Key: req.GetKey(),
		Value: req.GetValue(),
		Cf: req.GetCf(),
	}})

	err := server.storage.Write(req.GetContext(), entries)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted

	res :=  new(kvrpcpb.RawDeleteResponse)

	if server.storage == nil {
		res.Error = "storage not initialized"
		return res, errors.New("storage not initialized")
	}

	var entries []storage.Modify
	entries = append(entries, storage.Modify{Data: storage.Delete{
		Key: req.GetKey(),
		Cf: req.GetCf(),
	}})

	err := server.storage.Write(req.GetContext(), entries)
	if err != nil && err.Error() != "Key not found"{
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	res := new(kvrpcpb.RawScanResponse)

	if server.storage == nil {
		res.Error = "storage not initialized"
		return res, errors.New("storage not initialized")
	}
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()

	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	it := reader.IterCF(req.GetCf())
	defer it.Close()

	for it.Seek(req.GetStartKey()); it.Valid(); it.Next() {
		if uint32(len(res.Kvs)) >= req.GetLimit() {
			break
		}
		val, err := it.Item().Value()
		kvpair := new(kvrpcpb.KvPair)
		*kvpair = kvrpcpb.KvPair{
			Key: it.Item().Key(),
			Value: val,
		}
		if err != nil {
			kvpair.Error = new(kvrpcpb.KeyError)
			kvpair.Error.Abort = err.Error()
		}
		res.Kvs = append(res.Kvs,kvpair)
	}
	return res, nil
}
