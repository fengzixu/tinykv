package raftstore

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"

	"github.com/coocood/badger"
	"github.com/cznic/mathutil"
	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/lockstore"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/ngaut/unistore/tikv/mvcc"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
)

type DBBundle struct {
	db            *badger.DB
	lockStore     *lockstore.MemStore
	rollbackStore *lockstore.MemStore
	regionUndo    *sync.Map // map[uint64]*lockUndoLog
	memStoreMu    sync.Mutex
}

type lockUndoLog struct {
	refCnt      int32
	undoEntries atomic.Value // []*undoEntry
}

func (ll *lockUndoLog) getUndoEntries() []*undoEntry {
	es := ll.undoEntries.Load()
	if es == nil {
		return nil
	}
	return es.([]*undoEntry)
}

func (ll *lockUndoLog) resetUndoEntries() {
	ll.undoEntries.Store([]*undoEntry{})
}

func (ll *lockUndoLog) appendUndoEntries(entries []*undoEntry) {
	oldLog := ll.getUndoEntries()
	newLog := make([]*undoEntry, 0, len(oldLog)+len(entries))
	newLog = append(append(newLog, oldLog...), entries...)
	ll.undoEntries.Store(newLog)
}

type undoEntry struct {
	regionId  uint64
	index     uint64
	undoLocks []*undoLock
}

type undoLock struct {
	isSet bool
	key   []byte
	val   []byte
}

func (bundle *DBBundle) getLockUndoLog(region uint64) *lockUndoLog {
	v, _ := bundle.regionUndo.LoadOrStore(region, new(lockUndoLog))
	return v.(*lockUndoLog)
}

type DBSnapshot struct {
	Txn           *badger.Txn
	LockStore     *lockstore.MemStore
	RollbackStore *lockstore.MemStore
}

func NewDBSnapshot(db *DBBundle) *DBSnapshot {
	return &DBSnapshot{
		Txn:           db.db.NewTransaction(false),
		LockStore:     db.lockStore,
		RollbackStore: db.rollbackStore,
	}
}

type regionSnapshot struct {
	regionState *raft_serverpb.RegionLocalState
	txn         *badger.Txn
	lockSnap    *lockstore.MemStore
	term        uint64
	index       uint64
}

func (rs *regionSnapshot) snapLocks(start, end []byte, lockStore *lockstore.MemStore, undoLog *lockUndoLog) {
	rs.lockSnap = lockstore.NewMemStore(8 << 20)
	iter := lockStore.NewIterator()
	for iter.Seek(start); iter.Valid() && (len(end) == 0 || bytes.Compare(iter.Key(), end) < 0); iter.Next() {
		rs.lockSnap.Insert(iter.Key(), iter.Value())
	}

	undoEntries := undoLog.getUndoEntries()
	for i := len(undoEntries) - 1; i >= 0; i-- {
		undo := undoEntries[i]
		if undo.index <= rs.index {
			break
		}
		for j := len(undo.undoLocks) - 1; j >= 0; j-- {
			undoLock := undo.undoLocks[j]
			if undoLock.isSet {
				rs.lockSnap.Delete(undoLock.key)
			} else {
				rs.lockSnap.Insert(undoLock.key, undoLock.val)
			}
		}
	}
}

type Engines struct {
	kv       *DBBundle
	kvPath   string
	raft     *badger.DB
	raftPath string
}

func NewEngines(kvEngine *mvcc.DBBundle, raftEngine *badger.DB, kvPath, raftPath string) *Engines {
	return &Engines{
		kv: &DBBundle{
			db:            kvEngine.DB,
			lockStore:     kvEngine.LockStore,
			rollbackStore: kvEngine.RollbackStore,
			regionUndo:    new(sync.Map),
		},
		kvPath:   kvPath,
		raft:     raftEngine,
		raftPath: raftPath,
	}
}

func (en *Engines) newRegionSnapshot(regionId uint64) (snap *regionSnapshot, err error) {
	undoLog := en.kv.getLockUndoLog(regionId)

	// Acquire lockstore undo logs before snapshot db.
	// Otherwise apply worker may release undo logs after raft index in db snapshot.
	atomic.AddInt32(&undoLog.refCnt, 1)
	txn := en.kv.db.NewTransaction(false)
	snap = &regionSnapshot{
		regionState: new(raft_serverpb.RegionLocalState),
		txn:         txn,
	}

	snap.index, snap.term, err = getAppliedIdxTermForSnapshot(en.raft, txn, regionId)
	if err != nil {
		return nil, err
	}

	stateVal, err := getValueTxn(txn, RegionStateKey(regionId))
	if err != nil {
		return nil, err
	}
	if err := snap.regionState.Unmarshal(stateVal); err != nil {
		return nil, err
	}

	region := snap.regionState.GetRegion()
	start, end := rawDataStartKey(region.StartKey), rawRegionKey(region.EndKey)
	snap.snapLocks(start, end, en.kv.lockStore, undoLog)
	atomic.AddInt32(&undoLog.refCnt, -1)

	return snap, nil
}

func (en *Engines) WriteKV(wb *WriteBatch) error {
	return wb.WriteToKV(en.kv)
}

func (en *Engines) WriteRaft(wb *WriteBatch) error {
	return wb.WriteToRaft(en.raft)
}

func (en *Engines) SyncKVWAL() error {
	// TODO: implement
	return nil
}

func (en *Engines) SyncRaftWAL() error {
	// TODO: implement
	return nil
}

type WriteBatch struct {
	entries       []*badger.Entry
	lockEntries   []*badger.Entry
	undoEntries   []*undoEntry
	size          int
	safePoint     int
	safePointLock int
	safePointSize int
	safePointUndo int
}

func (wb *WriteBatch) NewUndoAt(region, index uint64) {
	wb.undoEntries = append(wb.undoEntries, &undoEntry{
		regionId: region,
		index:    index,
	})
}

func (wb *WriteBatch) Len() int {
	return len(wb.entries) + len(wb.lockEntries)
}

func (wb *WriteBatch) Set(key, val []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:   key,
		Value: val,
	})
	wb.size += len(key) + len(val)
}

func (wb *WriteBatch) SetLock(key, val []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: mvcc.LockUserMetaNone,
	})
	undo := wb.undoEntries[len(wb.undoEntries)-1]
	undo.undoLocks = append(undo.undoLocks, &undoLock{
		isSet: true,
		key:   key,
	})
}

func (wb *WriteBatch) DeleteLock(key []byte, val []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:      key,
		UserMeta: mvcc.LockUserMetaDelete,
	})
	if len(val) != 0 {
		undo := wb.undoEntries[len(wb.undoEntries)-1]
		undo.undoLocks = append(undo.undoLocks, &undoLock{
			key: key,
			val: val,
		})
	}
}

func (wb *WriteBatch) Rollback(key []byte) {
	wb.lockEntries = append(wb.lockEntries, &badger.Entry{
		Key:      key,
		UserMeta: mvcc.LockUserMetaRollback,
	})
}

func (wb *WriteBatch) SetWithUserMeta(key, val, useMeta []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key:      key,
		Value:    val,
		UserMeta: useMeta,
	})
	wb.size += len(key) + len(val) + len(useMeta)
}

func (wb *WriteBatch) Delete(key []byte) {
	wb.entries = append(wb.entries, &badger.Entry{
		Key: key,
	})
	wb.size += len(key)
}

func (wb *WriteBatch) SetMsg(key []byte, msg proto.Message) error {
	val, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	wb.Set(key, val)
	return nil
}

func (wb *WriteBatch) SetSafePoint() {
	wb.safePoint = len(wb.entries)
	wb.safePointLock = len(wb.lockEntries)
	wb.safePointUndo = len(wb.undoEntries)
	wb.safePointSize = wb.size
}

func (wb *WriteBatch) RollbackToSafePoint() {
	wb.entries = wb.entries[:wb.safePoint]
	wb.lockEntries = wb.lockEntries[:wb.safePointLock]
	wb.undoEntries = wb.undoEntries[:wb.safePointUndo]
	wb.size = wb.safePointSize
}

// WriteToKV flush WriteBatch to DB by three steps:
// 	1. Flush undo log.
//	2. Update lockstore. Because we have save undo log, ongoing snapshot can revert dirty locks by replay undo log.
// 	3. Write entries to badger. After save ApplyState to badger, subsequent regionSnapshot will start at new raft index.
// 	   So we can safely release all undo logs if there is no ongoing snapshot progress.
func (wb *WriteBatch) WriteToKV(bundle *DBBundle) error {
	if len(wb.undoEntries) > 0 {
		currRegion := wb.undoEntries[0].regionId
		undoLog := bundle.getLockUndoLog(currRegion)
		var undoBuf []*undoEntry
		for _, undo := range wb.undoEntries {
			if undo.regionId != currRegion {
				undoLog.appendUndoEntries(undoBuf)
				undoBuf = undoBuf[:0]
				undoLog = bundle.getLockUndoLog(undo.regionId)
				currRegion = undo.regionId
			}
			undoBuf = append(undoBuf, undo)
		}
		undoLog.appendUndoEntries(undoBuf)
	}

	var raftStates []*badger.Entry
	if len(wb.entries) > 0 {
		err := bundle.db.Update(func(txn *badger.Txn) error {
			var err1 error
			for _, entry := range wb.entries {
				if IsRaftStateKey(entry.Key) {
					raftStates = append(raftStates, entry)
					continue
				}
				if len(entry.UserMeta) == 0 && len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if len(wb.lockEntries) > 0 {
		bundle.memStoreMu.Lock()
		for _, entry := range wb.lockEntries {
			switch entry.UserMeta[0] {
			case mvcc.LockUserMetaRollbackByte:
				bundle.rollbackStore.Insert(entry.Key, []byte{0})
			case mvcc.LockUserMetaDeleteByte:
				if !bundle.lockStore.Delete(entry.Key) {
					panic("failed to delete key")
				}
			case mvcc.LockUserMetaRollbackGCByte:
				bundle.rollbackStore.Delete(entry.Key)
			default:
				if !bundle.lockStore.Insert(entry.Key, entry.Value) {
					panic("failed to insert key")
				}
			}
		}
		bundle.memStoreMu.Unlock()
	}

	if len(raftStates) > 0 {
		err := bundle.db.Update(func(txn *badger.Txn) error {
			var err1 error
			for _, entry := range raftStates {
				if err1 = txn.SetEntry(entry); err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	for _, undo := range wb.undoEntries {
		undoLog := bundle.getLockUndoLog(undo.regionId)
		if atomic.LoadInt32(&undoLog.refCnt) == 0 {
			undoLog.resetUndoEntries()
		}
	}

	return nil
}

func (wb *WriteBatch) WriteToRaft(db *badger.DB) error {
	if len(wb.entries) > 0 {
		err := db.Update(func(txn *badger.Txn) error {
			var err1 error
			for _, entry := range wb.entries {
				if len(entry.Value) == 0 {
					err1 = txn.Delete(entry.Key)
				} else {
					err1 = txn.SetEntry(entry)
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (wb *WriteBatch) MustWriteToKV(db *DBBundle) {
	err := wb.WriteToKV(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) MustWriteToRaft(db *badger.DB) {
	err := wb.WriteToRaft(db)
	if err != nil {
		panic(err)
	}
}

func (wb *WriteBatch) Reset() {
	wb.entries = wb.entries[:0]
	wb.lockEntries = wb.lockEntries[:0]
	wb.undoEntries = wb.undoEntries[:0]
	wb.size = 0
	wb.safePoint = 0
	wb.safePointLock = 0
	wb.safePointSize = 0
	wb.safePointUndo = 0
}

// Todo, the following code redundant to unistore/tikv/worker.go, just as a place holder now.

const delRangeBatchSize = 4096

const maxSystemTS = math.MaxUint64

func deleteRange(db *DBBundle, startKey, endKey []byte) error {
	// Delete keys first.
	keys := make([][]byte, 0, delRangeBatchSize)
	oldStartKey := mvcc.EncodeOldKey(startKey, maxSystemTS)
	oldEndKey := mvcc.EncodeOldKey(endKey, maxSystemTS)
	txn := db.db.NewTransaction(false)
	reader := dbreader.NewDBReader(startKey, endKey, txn, 0)
	keys = collectRangeKeys(reader.GetIter(), startKey, endKey, keys)
	keys = collectRangeKeys(reader.GetIter(), oldStartKey, oldEndKey, keys)
	reader.Close()
	if err := deleteKeysInBatch(db, keys, delRangeBatchSize); err != nil {
		return err
	}

	// Delete lock
	lockIte := db.lockStore.NewIterator()
	keys = keys[:0]
	keys = collectLockRangeKeys(lockIte, startKey, endKey, keys)
	return deleteLocksInBatch(db, keys, delRangeBatchSize)
}

func collectRangeKeys(it *badger.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func collectLockRangeKeys(it *lockstore.Iterator, startKey, endKey []byte, keys [][]byte) [][]byte {
	if len(endKey) == 0 {
		panic("invalid end key")
	}
	for it.Seek(startKey); it.Valid(); it.Next() {
		key := safeCopy(it.Key())
		if exceedEndKey(key, endKey) {
			break
		}
		keys = append(keys, key)
	}
	return keys
}

func deleteKeysInBatch(db *DBBundle, keys [][]byte, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		dbBatch := new(WriteBatch)
		for _, key := range batchKeys {
			dbBatch.Delete(key)
		}
		if err := dbBatch.WriteToKV(db); err != nil {
			return err
		}
	}
	return nil
}

func deleteLocksInBatch(db *DBBundle, keys [][]byte, batchSize int) error {
	for len(keys) > 0 {
		batchSize := mathutil.Min(len(keys), batchSize)
		batchKeys := keys[:batchSize]
		keys = keys[batchSize:]
		dbBatch := new(WriteBatch)
		for _, key := range batchKeys {
			dbBatch.DeleteLock(key, nil)
		}
		if err := dbBatch.WriteToKV(db); err != nil {
			return err
		}
	}
	return nil
}
