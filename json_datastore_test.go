package cloudypg

import (
	"context"
	"encoding/json"
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
	err := ds.Open(ctx, nil)
	require.NoError(t, err)

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

func TestJsonDatastoreOldData(t *testing.T) {
	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[datastore.TestItem](ctx, p, "testitems")

	// Create a table with the old schema
	sqlTableCreate := fmt.Sprintf(`
		CREATE TABLE  IF NOT EXISTS %v (
			id varchar(200) NOT NULL PRIMARY KEY,
			version integer DEFAULT 1,
			last_updated timestamp DEFAULT CURRENT_TIMESTAMP,
			date_created timestamp DEFAULT CURRENT_TIMESTAMP,
			data json
		);`, ds.table)

	conn, err := p.Acquire(ctx)
	require.NoError(t, err)

	_, err = conn.Exec(ctx, sqlTableCreate)
	require.NoError(t, err)

	item := &datastore.TestItem{
		ID:   "12345",
		Name: "OLD",
	}
	data, _ := json.Marshal(item)

	// Now insert a record with the old schema
	tag, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %v (id, data) VALUES ($1, $2)", ds.table), item.ID, data)
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())
	p.Return(ctx, conn)

	err = ds.Open(ctx, nil)
	require.NoError(t, err)

	item3, err := ds.Get(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, item3)
	require.Equal(t, item3.ID, item3.ID)

	meta, err := ds.GetMetadata(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, meta[0].Key, item.ID)
	require.Equal(t, meta[0].Version, int64(1))

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

	meta, err := ds.GetMetadata(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, meta[0].Key, item.ID)
	require.Equal(t, meta[0].Version, int64(1))

	// Now update
	item.Name = "Name2"
	err = ds.Save(ctx, item, item.ID)
	require.NoError(t, err)

	meta2, err := ds.GetMetadata(ctx, item.ID)
	require.NoError(t, err)
	require.NotNil(t, meta2)
	require.Equal(t, meta2[0].Key, item.ID)
	require.Equal(t, meta2[0].Version, int64(2))

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
	err := ds.Open(ctx, nil)
	require.NoError(t, err)

	items := []*testData{td, td2}
	ids := []string{td.ID, td2.ID}

	require.NoError(t, ds.SaveAll(ctx, items, ids))
}

func TestSaveAll(t *testing.T) {
	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[testData](ctx, p, "testitems")
	err := ds.Open(ctx, nil)
	require.NoError(t, err)

	items := make([]*testData, 10)
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		items[i] = &testData{
			ID:        fmt.Sprintf("item-%v", i),
			TimeStamp: strfmt.DateTime(time.Now()),
			Count:     RandomInt(10000),
		}
		keys[i] = items[i].ID
	}

	require.NoError(t, ds.SaveAll(ctx, items, keys), "SaveAll should not error")

	all, err := ds.GetAll(ctx)
	require.NoError(t, err)
	require.Len(t, all, 10, "GetAll should return 10 items")

	meta, err := ds.GetMetadata(ctx, keys...)
	require.NoError(t, err)
	require.Len(t, meta, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, int64(1), meta[i].Version)
	}
}

func TestJsonDataStoreQuery1(t *testing.T) {
	ctx := cloudy.StartContext()
	cfg := CreateDefaultPostgresqlContainer(t)

	connStr := ConnStringFrom(ctx, cfg)

	p := NewDedicatedPostgreSQLConnectionProvider(connStr)
	ds := NewJsonDatastore[testData](ctx, p, "testitems")
	err := ds.Open(ctx, nil)
	require.NoError(t, err)

	tQuery := time.Date(2000, 01, 01, 0, 0, 0, 0, time.Now().Location())
	isBefore := tQuery.Add(-10 * time.Second)
	isAfter := tQuery.Add(10 * time.Second)

	td1 := &testData{
		ID:        "1",
		TimeStamp: strfmt.DateTime(tQuery),
	}

	err = ds.Save(ctx, td1, td1.ID)
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
