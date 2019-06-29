package dspebble

import (
	"os"
	"testing"

	"reflect"

	"github.com/ipfs/go-datastore"
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
	key := datastore.NewKey("keks")
	data := []byte("hello world")
	// test put
	if err := ds.Put(key, data); err != nil {
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
	// TODO: query tests
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
