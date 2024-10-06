package cloudypg

import (
	"testing"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
	"github.com/stretchr/testify/require"
)

type TestItem struct {
	ID   string `json:"id"`
	Name string
}

func TestJsonDatastore(t *testing.T) {
	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[datastore.TestItem](ctx, p, "testitems")

	err := ds.Open(ctx, nil)
	require.NoError(t, err)

	item := &datastore.TestItem{
		ID:   "1234",
		Name: "MyName",
	}

	err = ds.Save(ctx, item, item.ID)
	require.NoError(t, err)

	item2, err := ds.Get(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, item2)

}
