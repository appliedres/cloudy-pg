package cloudypg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
	"github.com/appliedres/cloudy/datatype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ datatype.JsonDataStore[string] = (*JsonDataStore[string])(nil)

type JsonDataStore[T any] struct {
	provider      PostgresqlConnectionProvider
	table         string
	ConnectionKey pgContextKey
}

func NewJsonDatastore[T any](ctx context.Context, provider PostgresqlConnectionProvider, table string) *JsonDataStore[T] {
	return &JsonDataStore[T]{
		provider:      provider,
		table:         table,
		ConnectionKey: pgContextKey(table),
	}
}

// Open will open the datastore for usage. This should
// only be done once per datastore
func (ds *JsonDataStore[T]) Open(ctx context.Context, config any) error {
	cloudy.Info(ctx, "Openning UntypedPostgreSqlJsonDataStore %v", ds.table)
	conn, err := ds.checkConnection(ctx)
	ds.returnConnection(ctx, conn)

	return err
}

// Close should be called to cleanly close the datastore
func (ds *JsonDataStore[T]) Close(ctx context.Context) error {
	return nil
}

func (ds *JsonDataStore[T]) returnConnection(ctx context.Context, conn *pgxpool.Conn) {
	if conn == nil {
		fmt.Println("RETURNING NIL CONNECTION")
		return
	}

	// Dont get rid of the connection if it cam from the context
	obj := ctx.Value(ds.ConnectionKey)
	if obj != nil && obj == conn {
		return
	}

	if ds.provider != nil {
		ds.provider.Return(ctx, conn)
	} else {
		conn.Release()
	}

}

func (ds *JsonDataStore[T]) checkConnection(ctx context.Context) (*pgxpool.Conn, error) {
	var conn *pgxpool.Conn
	var err error

	// Check to see if there is a connection in the context. If not then add one
	obj := ctx.Value(ds.ConnectionKey)
	if obj != nil {
		return obj.(*pgxpool.Conn), nil
	}

	if ds.provider == nil {
		return nil, errors.New("no connection provider")
	}

	conn, err = ds.provider.Acquire(ctx)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to aquire connection to database: %v\n", err)
	}

	// Table does not exist
	sqlTableCreate := fmt.Sprintf(`
		CREATE TABLE  IF NOT EXISTS %v (
			id varchar(200) NOT NULL PRIMARY KEY, 
			data json NOT NULL
		);`, ds.table)

	_, err = conn.Exec(ctx, sqlTableCreate)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to create table: %v, %v\n", ds.table, err)
	}

	return conn, nil
}

// Save stores an item in the datastore. There is no difference
// between an insert and an update.
func (ds *JsonDataStore[T]) Save(ctx context.Context, item *T, key string) error {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer ds.returnConnection(ctx, conn)

	data, err := toByte(item)
	if err != nil {
		return fmt.Errorf("error converting to json, %v", err)
	}

	sqlUpsert := fmt.Sprintf(`INSERT INTO %v (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2;`, ds.table)
	_, err = conn.Exec(ctx, sqlUpsert, key, data)
	if err != nil {
		return fmt.Errorf("database error, %v", err)
	}

	return nil
}

// Get retrieves an item by it's unique id
func (ds *JsonDataStore[T]) Get(ctx context.Context, key string) (*T, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer ds.returnConnection(ctx, conn)
	sql := fmt.Sprintf(`SELECT data FROM %v where ID=$1`, ds.table)
	row := conn.QueryRow(ctx, sql, key)

	var jsonResult []byte
	err = row.Scan(&jsonResult)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("error scaning into struct : %v", err)
	}
	return fromByte[T](jsonResult)
}

// Gets all the items in the store.
func (ds *JsonDataStore[T]) GetAll(ctx context.Context) ([]*T, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer ds.returnConnection(ctx, conn)

	sql := fmt.Sprintf(`SELECT data FROM %v`, ds.table)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}
	rtn, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*T, error) {
		var jsonResult []byte
		err = rows.Scan(&jsonResult)
		if err != nil {
			return nil, err
		}
		return fromByte[T](jsonResult)
	})
	return rtn, nil
}

// Deletes an item
func (ds *JsonDataStore[T]) Delete(ctx context.Context, key string) error {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer ds.returnConnection(ctx, conn)

	sqlDelete := fmt.Sprintf(`DELETE FROM %v where ID=$1`, ds.table)
	_, err = conn.Exec(ctx, sqlDelete, key)
	if err != nil {
		return fmt.Errorf("error deleteing %v from %v : %v", key, ds.table, err)
	}
	return nil
}

// Checks to see if a key exists
func (ds *JsonDataStore[T]) Exists(ctx context.Context, key string) (bool, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return false, err
	}
	defer ds.returnConnection(ctx, conn)

	sqlExists := fmt.Sprintf(`SELECT ID FROM %v where ID=$1`, ds.table)
	rows, err := conn.Query(ctx, sqlExists, key)
	if err != nil {
		return false, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	defer rows.Close()
	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func (ds *JsonDataStore[T]) Count(ctx context.Context, query *datastore.SimpleQuery) (int, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return -1, err
	}
	defer ds.returnConnection(ctx, conn)

	query.Colums = []string{}
	sql := new(PgQueryConverter).Convert(query, ds.table)
	sql = strings.Replace(sql, "SELECT data", "SELECT COUNT(*) as cnt", 1)
	row := conn.QueryRow(ctx, sql)
	var cnt int
	err = row.Scan(&cnt)
	if err != nil {
		return -1, fmt.Errorf("error querying database: %v", err)
	}
	return cnt, nil
}

// Sends a simple Query
func (ds *JsonDataStore[T]) Query(ctx context.Context, query *datastore.SimpleQuery) ([]*T, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer ds.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, ds.table)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("error querying database : %v", err)
	}
	rtn, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*T, error) {
		var jsonResult []byte
		err = rows.Scan(&jsonResult)
		if err != nil {
			return nil, err
		}
		return fromByte[T](jsonResult)
	})
	return rtn, err
}

func (ds *JsonDataStore[T]) QueryAndUpdate(ctx context.Context, query *datastore.SimpleQuery, updater func(ctx context.Context, items []*T) ([]*T, error)) ([]*T, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer ds.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, ds.table)

	var updated []*T

	// All this runs in a single transaction
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		sql = sql + " FOR UPDATE"
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			return err
		}

		rtn, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*T, error) {
			var jsonResult []byte
			err = rows.Scan(&jsonResult)
			if err != nil {
				return nil, err
			}
			return fromByte[T](jsonResult)
		})

		updated, err = updater(ctx, rtn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func (ds *JsonDataStore[T]) QueryAsMap(ctx context.Context, query *datastore.SimpleQuery) ([]map[string]any, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer ds.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, ds.table)

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}
	rtn, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (map[string]interface{}, error) {
		return pgx.RowToMap(row)
	})
	return rtn, err
}

func (ds *JsonDataStore[T]) QueryTable(ctx context.Context, query *datastore.SimpleQuery) ([][]interface{}, error) {
	conn, err := ds.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer ds.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, ds.table)
	// Fix the SQL
	// sql = strings.Replace(sql, "SELECT data ,", "SELECT ", 1)

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	defer rows.Close()
	var rtn [][]interface{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return rtn, err
		}
		rtn = append(rtn, vals)
	}
	return rtn, nil
}
