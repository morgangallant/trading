package main

import (
	"fmt"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Iterator interface {
	Next() bool
	Key() string
	Value() []byte
	Close() error
}

type BatchMutation interface {
	Put(key string, value []byte)
	Delete(key string)
}

type Store interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
	BeginBatch() BatchMutation
	CommitBatch(batch BatchMutation) error
	RangeScan(start, end string) Iterator
	PrefixScan(prefix string) Iterator
}

var (
	ErrKeyNotFound   = fmt.Errorf("key not found")
	ErrKeyTooLarge   = fmt.Errorf("key too large")
	ErrValueTooLarge = fmt.Errorf("value too large")
)

const (
	MaxKeySize   = 1 << 14 // 16 KiB
	MaxValueSize = 1 << 16 // 64 KiB
)

func CheckSizes(key string, value []byte) error {
	switch {
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	case len(value) > MaxValueSize:
		return ErrValueTooLarge
	default:
		return nil
	}
}

type pstore struct {
	ldb *leveldb.DB
}

func (ps *pstore) Get(key string) ([]byte, error) {
	val, err := ps.ldb.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("leveldb error: %w", err)
	}
	if val == nil {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

func (ps *pstore) Put(key string, value []byte) error {
	if err := CheckSizes(key, value); err != nil {
		return err
	}
	if err := ps.ldb.Put([]byte(key), value, nil); err != nil {
		return fmt.Errorf("leveldb error: %w", err)
	}
	return nil
}

func (ps *pstore) Delete(key string) error {
	if err := ps.ldb.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("leveldb error: %w", err)
	}
	return nil
}

type pbatch struct {
	errmu sync.Mutex
	err   error
	batch *leveldb.Batch
}

func (pb *pbatch) Put(key string, value []byte) {
	pb.errmu.Lock()
	defer pb.errmu.Unlock()
	if pb.err != nil {
		return
	}
	if err := CheckSizes(key, value); err != nil {
		pb.err = err
		return
	}
	pb.batch.Put([]byte(key), value)
}

func (pb *pbatch) Delete(key string) {
	pb.errmu.Lock()
	defer pb.errmu.Unlock()
	if pb.err != nil {
		return
	}
	pb.batch.Delete([]byte(key))
}

func (ps *pstore) BeginBatch() BatchMutation {
	return &pbatch{batch: new(leveldb.Batch)}
}

func (ps *pstore) CommitBatch(batch BatchMutation) error {
	b, ok := batch.(*pbatch)
	if !ok {
		return fmt.Errorf("invalid batch mutation type: %T", batch)
	}
	b.errmu.Lock()
	defer b.errmu.Unlock()
	if b.err != nil {
		return b.err
	}
	if err := ps.ldb.Write(b.batch, nil); err != nil {
		return fmt.Errorf("leveldb error: %w", err)
	}
	return nil
}

type piter struct {
	it     iterator.Iterator
	closed bool
}

func (pi *piter) Next() bool {
	if pi.closed {
		panic("next() called on closed iterator")
	}
	return pi.it.Next()
}

func (pi *piter) Key() string {
	return string(pi.it.Key())
}

func (pi *piter) Value() []byte {
	return pi.it.Value()
}

func (pi *piter) Close() error {
	pi.closed = true
	pi.it.Release()
	if err := pi.it.Error(); err != nil {
		return fmt.Errorf("leveldb error: %w", err)
	}
	return nil
}

func (ps *pstore) RangeScan(start, end string) Iterator {
	var sb, eb []byte
	if start != "" {
		sb = []byte(start)
	}
	if end != "" {
		eb = []byte(end)
	}
	return &piter{
		it: ps.ldb.NewIterator(&util.Range{
			Start: sb,
			Limit: eb,
		}, nil),
		closed: false,
	}
}

func (ps *pstore) PrefixScan(prefix string) Iterator {
	return &piter{
		it:     ps.ldb.NewIterator(util.BytesPrefix([]byte(prefix)), nil),
		closed: false,
	}
}

func NewPersistantStore(path string) (Store, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		BlockCacheCapacity: 512 * opt.MiB,             // 512 MiB LRU Cache
		Filter:             filter.NewBloomFilter(10), // 10-bit Bloom Filter
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}
	return &pstore{db}, nil
}
