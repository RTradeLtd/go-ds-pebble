package dspebble

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/petermattis/pebble"
)

var (
	_ datastore.Batching            = (*Datastore)(nil)
	_ datastore.PersistentDatastore = (*Datastore)(nil)
)

// Datastore implements a pebble backed ipfs datastore
type Datastore struct {
	db       *pebble.DB
	walStats bool
	withSync bool
}

// NewDatastore instantiates a new pebble datastore
func NewDatastore(path string, opts *pebble.Options, withSync bool) (*Datastore, error) {
	ds, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &Datastore{ds, false, withSync}, nil
}

// Put is used to store a value named by key
func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Set(key.Bytes(), value, &pebble.WriteOptions{Sync: d.withSync})
}

// Delete removes the value for given `key`.
func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Delete(key.Bytes(), &pebble.WriteOptions{Sync: d.withSync})
}

// Get is used to return a value named key from our datastore
func (d *Datastore) Get(key datastore.Key) ([]byte, error) {
	return d.db.Get(key.Bytes())
}

// Has is used to check if we have a value named key in our datastore
func (d *Datastore) Has(key datastore.Key) (bool, error) {
	return datastore.GetBackedHas(d, key)
}

// GetSize is used to get the size of a value named key
func (d *Datastore) GetSize(key datastore.Key) (int, error) {
	return datastore.GetBackedSize(d, key)
}

// Query is used to search a datastore for keys, and optionally values
// matching a given query
func (d *Datastore) Query(q query.Query) (query.Results, error) {
	var (
		entries []query.Entry
		snap    = d.db.NewSnapshot()
		iter    = snap.NewIter(nil)
	)
	defer snap.Close()
	defer iter.Close()
	// TODO(postables): currently if we do not specify the initial `/`
	// then all specific queries  will not work. So we need to make sure that the "prefix"
	// we specify, includes the `/` for ipfs datastore keys, otherwise it will not work
	if q.Prefix != "" && q.Prefix[0] != '/' {
		q.Prefix = fmt.Sprintf("/%s", q.Prefix)
	}
	// thanks to petermattis for suggestion
	// see https://github.com/petermattis/pebble/issues/168#issuecomment-507042838
	for valid := iter.SeekGE([]byte(q.Prefix)); valid; valid = iter.Next() {
		entry := query.Entry{}
		key := iter.Key()
		entry.Key = string(key)
		if !q.KeysOnly {
			entry.Value = iter.Value()
		}
		entries = append(entries, entry)
	}
	results := query.ResultsWithEntries(q, entries)
	return results, nil
}

// Batch returns a batchable datastore useful for combining
// many operations into one
func (d *Datastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(d), nil
}

// DiskUsage returns the space used by our datastore in bytes
// it does not include the WAL (Write Ahead Log) size
// and only includes total size from all the "levels"
func (d *Datastore) DiskUsage() (uint64, error) {
	var totalSize uint64
	for _, level := range d.db.Metrics().Levels {
		totalSize = totalSize + level.Size
	}
	if d.walStats {
		totalSize = totalSize + d.db.Metrics().WAL.Size
	}
	return totalSize, nil
}

// ToggleWALStats is used to toggle reporting of
// WAL statistics when runnning DiskUsage
func (d *Datastore) ToggleWALStats() {
	d.walStats = !d.walStats
}

// Close is used to terminate our datastore connection
func (d *Datastore) Close() error {
	return d.db.Close()
}
