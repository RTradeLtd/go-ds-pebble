package dspebble

import (
	"os"
	"testing"

	"reflect"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

func Test_NewDatastore(t *testing.T) {
	defer os.RemoveAll("./tmp")
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Success", args{"./tmp"}, false},
		{"Fail", args{"/root/toor"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := NewDatastore(tt.args.path, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewDatastore() err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if err := ds.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func Test_Datastore(t *testing.T) {
	defer os.RemoveAll("./tmp")
	ds, err := NewDatastore("./tmp", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	key := datastore.NewKey("kek")
	key2 := datastore.NewKey("keks")
	key3 := datastore.NewKey("keks3")
	data := []byte("hello world")
	// test first put
	if err := ds.Put(key, data); err != nil {
		t.Fatal(err)
	}
	// test second put
	if err := ds.Put(key2, data); err != nil {
		t.Fatal(err)
	}
	// test third put
	if err := ds.Put(key3, data); err != nil {
		t.Fatal(err)
	}
	// test get
	retData, err := ds.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data, retData) {
		t.Fatal("returned data not equal")
	}
	// test get size
	size, err := ds.GetSize(key)
	if err != nil {
		t.Fatal(err)
	}
	if size != len(data) {
		t.Fatal("bad size returned")
	}
	// test an empty prefix query search
	// this should iterate through all items in the datastore
	results, err := ds.Query(query.Query{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) < 2 {
		t.Fatal("bad number of results found")
	}
	// test a prefixed query search for keks, this should
	// only return at most 2 results
	results, err = ds.Query(query.Query{Prefix: "keks"})
	if err != nil {
		t.Fatal(err)
	}
	res, err = results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatal("bad number of results found")
	}
	// test has
	if has, err := ds.Has(key); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Fatal("should have key")
	}
	// test delete
	if err := ds.Delete(key); err != nil {
		t.Fatal(err)
	}
	// test get after delete
	if _, err := ds.Get(key); err == nil {
		t.Fatal("expected error")
	}
	// test has after delete
	if has, err := ds.Has(key); err != nil {
		t.Fatal(err)
	} else if has {
		t.Fatal("should not have key")
	}
	// test get size after delete
	if size, err := ds.GetSize(key); err == nil {
		t.Fatal("expected error")
	} else if size > 0 {
		t.Fatal("size should be 0")
	}
}
