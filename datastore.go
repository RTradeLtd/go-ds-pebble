package dspebble

import (
	"errors"

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
	resBuilder := query.NewResultBuilder(q)
	var iter *pebble.Iterator
	if q.Prefix == "" {
		iter = d.db.NewSnapshot().NewIter(nil)
	} else {
		iter = d.db.NewSnapshot().NewIter(&pebble.IterOptions{LowerBound: []byte(q.Prefix)})
	}
	// get the very first result
	if !iter.First() {
		return nil, errors.New("no results found")
	}
	result := query.Result{}
	result.Key = string(iter.Key())
	if !q.KeysOnly {
		result.Value = iter.Value()
	}
	select {
	case resBuilder.Output <- result:
	default:
		break
	}
	// search through remaining keys
	for succ := iter.Next(); succ == true; succ = iter.Next() {
		result := query.Result{}
		val := iter.Key()
		result.Key = string(val)
		if !q.KeysOnly {
			result.Value, result.Error = d.Get(datastore.NewKey(result.Key))
		}
		select {
		case resBuilder.Output <- result:
		default:
			continue
		}
	}
	// close the result builder
	if err := resBuilder.Process.Close(); err != nil {
		return nil, err
	}
	return resBuilder.Results(), nil
}

// Close is used to terminate our datastore connection
func (d *Datastore) Close() error {
	return d.db.Close()
}
