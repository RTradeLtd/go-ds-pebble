package dspebble

import (
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/petermattis/pebble"
)

var (
	_ datastore.Datastore = (*Datastore)(nil)
)

// Datastore implements a pebble backed ipfs datastore
type Datastore struct {
	db *pebble.DB
}

// NewDatastore instantiates a new pebble datastore
func NewDatastore(path string, opts *pebble.Options) (*Datastore, error) {
	ds, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &Datastore{ds}, nil
}

// Put is used to store a value named by key
func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Set(key.Bytes(), value, &pebble.WriteOptions{Sync: false})
}

// Delete removes the value for given `key`.
func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Delete(key.Bytes(), &pebble.WriteOptions{Sync: false})
}

// Get is used to return a value named key from our datastore
func (d *Datastore) Get(key datastore.Key) ([]byte, error) {
	return d.db.Get(key.Bytes())
}

// Has is used to check if we have a value named key in our datastore
func (d *Datastore) Has(key datastore.Key) (bool, error) {
	_, err := d.Get(key)
	if err != nil && err != pebble.ErrNotFound {
		return false, err
	}
	if err != nil && err == pebble.ErrNotFound {
		return false, nil
	}
	return true, nil
}

// GetSize is used to get the size of a value named key
func (d *Datastore) GetSize(key datastore.Key) (int, error) {
	data, err := d.Get(key)
	if err != nil {
		return 0, err
	}
	return len(data), nil
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

// Close is used to terminate our datastore connection
func (d *Datastore) Close() error {
	return d.db.Close()
}
