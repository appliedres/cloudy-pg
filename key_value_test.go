package cloudypg

import (
	"testing"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/keyvalue"
)

func TestKeyValue(t *testing.T) {
	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	conn, err := Connect(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	kv, err := NewKeyValueStore(ctx, "keyvaluetest", conn)
	if err != nil {
		t.Fatal(err)
	}
	keyvalue.TestWritableKVStore(t, kv, keyvalue.TestStoreNormalForms)
}
