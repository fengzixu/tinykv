package standalone_storage

import (
	"fmt"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	Engines       *engine_util.Engines
	StorageReader *StandAloneStorageReader
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvPath := filepath.Join(conf.DBPath, "kv")
	db := engine_util.CreateDB("kv", conf)
	return &StandAloneStorage{
		Engines: engine_util.NewEngines(db, nil, kvPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.Engines.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{
		DB:  s.Engines.Kv,
		Txn: s.Engines.Kv.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if batch == nil || len(batch) == 0 {
		return fmt.Errorf("The data you want to write is null")
	}

	wb := new(engine_util.WriteBatch)
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			wb.SetCF(modify.Cf(), modify.Key(), modify.Value())
		case storage.Delete:
			wb.DeleteCF(modify.Cf(), modify.Key())
		}
	}

	return s.Engines.WriteKV(wb)
}

// StandAloneStorageReader implements all interfaces of storage.StorageReader. It was used in reading data
// from kv store.
type StandAloneStorageReader struct {
	DB  *badger.DB
	Txn *badger.Txn
}

func (ssr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(ssr.DB, cf, key)
}

func (ssr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, ssr.Txn)
}

func (ssr *StandAloneStorageReader) Close() {
	ssr.Txn.Discard()
}
