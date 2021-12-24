package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/Connor1996/badger"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
	conf config.Config
}

type StandAloneStorageReader struct {
	s *StandAloneStorage
	tx *badger.Txn
}

func NewStandAloneStorageReader(s *StandAloneStorage) (*StandAloneStorageReader, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("storage is not initialized")
	}
	sr := new(StandAloneStorageReader)
	sr.s = s
	return sr, nil
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte)([]byte, error) {
	if sr.s.db == nil {
		log.Fatal("storage is not initialized")
		return nil, errors.New("db is not initialized")
	}
	return engine_util.GetCF(sr.s.db, cf, key)
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if sr.s.db == nil {
		log.Fatal("db is not initialized")
		return nil
	}
	sr.tx = sr.s.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, sr.tx)
}

func (sr *StandAloneStorageReader) Close() {
	sr.tx.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := new(StandAloneStorage)
	s.conf = *conf
	return nil
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.db == nil {
		s.db = engine_util.CreateDB(s.conf.DBPath, s.conf.Raft)
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1)

	return NewStandAloneStorageReader(s)
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).

	wb := new(engine_util.WriteBatch)
	for _, b := range batch {
		if b.Value() == nil {
			// delete the KV
			wb.DeleteCF(b.Cf(), b.Key())
		} else {
			// add the kv
			wb.SetCF(b.Cf(), b.Key(), b.Value())
		}
	}
	return wb.WriteToDB(s.db)
}

