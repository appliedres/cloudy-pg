package cloudypg

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
)

type TestItem struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Parent string `json:"parent"`
}

func TestRecursiveParentQuery(t *testing.T) {

	root := &TestItem{
		ID:   "1",
		Name: "Root",
	}

	level1 := &TestItem{
		ID:     "2",
		Name:   "Level 1",
		Parent: root.ID,
	}

	level2 := &TestItem{
		ID:     "3",
		Name:   "Level 2",
		Parent: level1.ID,
	}

	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[TestItem](ctx, p, "testitems")

	// Save
	require.NoError(t, ds.Save(ctx, root, root.ID))
	require.NoError(t, ds.Save(ctx, level1, level1.ID))
	require.NoError(t, ds.Save(ctx, level2, level2.ID))

	// Query
	t.Run("Query Up", func(t *testing.T) {
		q := datastore.NewQuery()
		q.Conditions.Equals("id", level2.ID)
		q.Recurse("parent", "id")
		items, err := ds.Query(ctx, q)
		require.NoError(t, err)
		require.Len(t, items, 3)
		require.Equal(t, items[0].ID, level2.ID)
		require.Equal(t, items[1].ID, level1.ID)
		require.Equal(t, items[2].ID, root.ID)
	})

	// Now check DOWN
	t.Run("Query Down", func(t *testing.T) {
		q := datastore.NewQuery()
		q.Conditions.Equals("id", root.ID)
		q.Recurse("id", "parent")
		items, err := ds.Query(ctx, q)
		require.NoError(t, err)
		require.Len(t, items, 3)
		require.Equal(t, items[0].ID, root.ID)
		require.Equal(t, items[1].ID, level1.ID)
		require.Equal(t, items[2].ID, level2.ID)
	})

	// Noow check down in the middle

	t.Run("Query Down", func(t *testing.T) {
		q := datastore.NewQuery()
		q.Conditions.Equals("id", level1.ID)
		q.Recurse("id", "parent")
		items, err := ds.Query(ctx, q)
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.Equal(t, items[0].ID, level1.ID)
		require.Equal(t, items[1].ID, level2.ID)
	})

	t.Run("Query Down", func(t *testing.T) {
		q := datastore.NewQuery()
		q.Conditions.Equals("name", level1.Name)
		q.Recurse("id", "parent")
		items, err := ds.Query(ctx, q)
		require.NoError(t, err)
		require.Len(t, items, 2)
		require.Equal(t, items[0].ID, level1.ID)
		require.Equal(t, items[1].ID, level2.ID)
	})

	t.Run("Query Down and DELETE", func(t *testing.T) {
		q := datastore.NewQuery()
		q.Conditions.Equals("id", root.ID)
		q.Recurse("id", "parent")
		ids, err := ds.DeleteQuery(ctx, q)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, ids[0], root.ID)
		require.Equal(t, ids[1], level1.ID)
		require.Equal(t, ids[2], level2.ID)

		all, err := ds.GetAll(ctx)
		require.NoError(t, err)
		require.Empty(t, all)
	})
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

func TestIDWithDash(t *testing.T) {
	td := &testData{
		ID:        "uvm-j8oxaig3z9g",
		TimeStamp: strfmt.DateTime(time.Now()),
		Count:     RandomInt(10000),
	}
	td2 := &testData{
		ID:        "uvm-j8oxaig3z9g2",
		TimeStamp: strfmt.DateTime(time.Now()),
		Count:     RandomInt(10000),
	}

	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[testData](ctx, p, "testitems")

	items := []*testData{td, td2}
	ids := []string{td.ID, td2.ID}

	require.NoError(t, ds.SaveAll(ctx, items, ids))
}
func TestSaveAll(t *testing.T) {
	td := &testData{
		ID:        "uvm-j8oxaig3z9g",
		TimeStamp: strfmt.DateTime(time.Now()),
		Count:     RandomInt(10000),
	}

	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[testData](ctx, p, "testitems")

	require.NoError(t, ds.Save(ctx, td, td.ID))
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

func TestJsonDatastoreQueryAndUpdate(t *testing.T) {
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

	q := datastore.NewQuery()
	q.Conditions.Equals("id", "1234")

	items, err := ds.QueryAndUpdate(ctx, q, func(ctx context.Context, items []*datastore.TestItem) ([]*datastore.TestItem, error) {
		items[0].Name = "Updated"
		err := ds.Save(ctx, items[0], items[0].ID)
		return items, err
	})
	require.NoError(t, err)
	require.NotEmpty(t, items)

	item2, err := ds.Get(ctx, item.ID)
	require.NoError(t, err)
	require.Equal(t, item2.Name, "Updated")

}
