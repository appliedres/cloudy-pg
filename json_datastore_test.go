package cloudypg

import (
	"fmt"
	"testing"
	"time"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
	"github.com/go-openapi/strfmt"
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

func TestJsonDataStoreQuery1(t *testing.T) {
	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[testData](ctx, p, "testitems")

	tQuery := time.Date(2000, 01, 01, 0, 0, 0, 0, time.Now().Location())
	isBefore := tQuery.Add(-10 * time.Second)
	isAfter := tQuery.Add(10 * time.Second)

	td1 := &testData{
		ID:        "1",
		TimeStamp: strfmt.DateTime(tQuery),
	}

	err := ds.Save(ctx, td1, td1.ID)
	require.NoError(t, err)

	q := datastore.NewQuery()
	q.Conditions.Before("TimeStamp", isAfter)

	items, err := ds.Query(ctx, q)
	require.NoError(t, err)
	fmt.Printf("Checking %v is Before %v: %v\n", tQuery, isAfter, len(items) > 0)

	q2 := datastore.NewQuery()
	q2.Conditions.Before("TimeStamp", isBefore)
	require.True(t, len(items) > 0)

	items2, err := ds.Query(ctx, q2)
	require.NoError(t, err)
	fmt.Printf("Checking %v is Before %v: %v\n", tQuery, isBefore, len(items2) > 0)
	require.False(t, len(items2) > 0)

	q3 := datastore.NewQuery()
	q3.Conditions.After("TimeStamp", isAfter)

	items3, err := ds.Query(ctx, q3)
	require.NoError(t, err)
	fmt.Printf("Checking %v is After %v: %v\n", tQuery, isAfter, len(items3) > 0)
	require.False(t, len(items3) > 0)

	q4 := datastore.NewQuery()
	q4.Conditions.After("TimeStamp", isBefore)

	items4, err := ds.Query(ctx, q4)
	require.NoError(t, err)
	fmt.Printf("Checking %v is After %v: %v\n", tQuery, isBefore, len(items4) > 0)
	require.True(t, len(items4) > 0)

}
