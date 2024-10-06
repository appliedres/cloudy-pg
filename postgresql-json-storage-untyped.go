package cloudypg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
)

const PostgresProviderID = "postgresql"

func init() {
	datastore.JsonDataStoreProviders.Register(PostgresProviderID, &PostgreSqlJsonDataStoreFactory{})
}

type PostgreSqlJsonDataStoreFactory struct {
	PostgreSqlConfig
}

func (c *PostgreSqlJsonDataStoreFactory) Create(cfg interface{}) (datastore.UntypedJsonDataStore, error) {
	//TODO: FIXME
	config := cfg.(*PostgreSqlConfig)
	uconfig := &UntypedPostgreSqlConfig{
		PostgreSqlConfig: *config,
	}

	result := NewUntypedPostgreSqlJsonDataStore(context.Background(), uconfig)
	return result, nil
	// return &UntypedPostgreSqlJsonDataStore{
	// 	connectionString: config.GetConnectionString(),
	// 	Database:         config.Database,
	// 	ConnectionKey:    pgContextKey(config.Table),
	// }, nil
}

func (c *PostgreSqlJsonDataStoreFactory) FromEnv(env *cloudy.Environment) (interface{}, error) {
	cfg := &PostgreSqlConfig{}

	cfg.Connection = env.Get("Connection")
	cfg.User = env.Get("User")
	cfg.Host = env.Get("Host")
	cfg.Password = env.Get("Password")
	cfg.Database = env.Get("Database")

	// Check that either Connection or (user,Host,Pass, database) is present
	if cfg.Connection != "" || (cfg.User != "" && cfg.Host != "" && cfg.Password != "" && cfg.Database != "") {
		return nil, errors.New("connection or User,Host,Password,Database must be specified")
	}

	return cfg, nil
}

func (c *PostgreSqlJsonDataStoreFactory) CreateJsonDatastore(ctx context.Context, typename string, prefix string, idField string) datastore.UntypedJsonDataStore {
	return NewUntypedPostgreSqlJsonDataStore(ctx, &UntypedPostgreSqlConfig{
		PostgreSqlConfig{
			Connection: c.GetConnectionString(),
			Table:      typename,
			Database:   c.Database,
		},
	})
}

type UntypedPostgreSqlConfig struct {
	PostgreSqlConfig
}

var _ datastore.UntypedJsonDataStore = (*UntypedPostgreSqlJsonDataStore)(nil)

type UntypedPostgreSqlJsonDataStore struct {
	// connectionString string
	// client           *pgx.Conn
	// Database      string
	table         string
	ConnectionKey pgContextKey
	// pool          *pgxpool.Pool
	provider PostgresqlConnectionProvider
	onCreate func(ctx context.Context, ds datastore.UntypedJsonDataStore) error
}

func NewUntypedPostgreSqlJsonDataStore(ctx context.Context, config *UntypedPostgreSqlConfig) *UntypedPostgreSqlJsonDataStore {
	connstr := ConnectionString(config.Host, config.User, config.Password, config.Database, int(config.Port))
	provider := NewDedicatedPostgreSQLConnectionProvider(connstr)

	// Generate Conne
	return &UntypedPostgreSqlJsonDataStore{
		// connectionString: config.GetConnectionString(),
		provider:      provider,
		table:         config.Table,
		ConnectionKey: pgContextKey(config.Table),
	}
}

func NewUntypedPostgreSqlJsonDataStoreWithProvider(ctx context.Context, table string, provider PostgresqlConnectionProvider) *UntypedPostgreSqlJsonDataStore {
	// Generate Conne
	return &UntypedPostgreSqlJsonDataStore{
		table:         table,
		provider:      provider,
		ConnectionKey: pgContextKey(table),
	}
}

// SAME
func (m *UntypedPostgreSqlJsonDataStore) Open(ctx context.Context, config interface{}) error {
	cloudy.Info(ctx, "Openning UntypedPostgreSqlJsonDataStore %v", m.table)
	conn, err := m.checkConnection(ctx)
	m.returnConnection(ctx, conn)

	return err
}

// For Transactions and long operations with the datastore consider
// Creating a new context with the connection stored as an attribute
// This will keep the connection from being returned to the pool
// If you do this you must explicitly end the connection.
//
// TODO: Determine if this should be safe to call multiple times. If
//
//	so then we need a reference counter or something
//
// SAME
func (m *UntypedPostgreSqlJsonDataStore) NewConnectionContext(ctx context.Context) (context.Context, error) {
	// See if there is already a connection
	obj := ctx.Value(m.ConnectionKey)
	if obj != nil {
		return ctx, nil
	}

	key := pgContextKey(m.table)
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return ctx, cloudy.Error(ctx, "Could not create connection, %v", err)
	}

	return context.WithValue(ctx, key, conn), nil
}

func (m *UntypedPostgreSqlJsonDataStore) ReturnConnectionContext(ctx context.Context) {
	// Check to see if there is a connection in the context. If not then add one
	obj := ctx.Value(m.ConnectionKey)
	if obj != nil {
		conn := obj.(*pgxpool.Conn)
		conn.Release()
	}
}

func (m *UntypedPostgreSqlJsonDataStore) Close(ctx context.Context) error {
	// if m.pool != nil {
	// 	m.pool.Close()
	// }
	return nil
}

func (m *UntypedPostgreSqlJsonDataStore) OnCreate(fn func(ctx context.Context, ds datastore.UntypedJsonDataStore) error) {
	m.onCreate = fn
}

// Save an item to the MongoDB. This is implemented as an Upsert to this will work
// for new items as well as updates.
func (m *UntypedPostgreSqlJsonDataStore) Save(ctx context.Context, item []byte, key string) error {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer m.returnConnection(ctx, conn)

	// modelJson, err := json.Marshal(item)
	// if err != nil {
	// 	return cloudy.Error(ctx, "Error marshalling item into json : %v", err)
	// }

	sqlUpsert := fmt.Sprintf(`INSERT INTO %v (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2;`, m.table)
	_, err = conn.Exec(ctx, sqlUpsert, key, item)
	if err != nil {
		return cloudy.Error(ctx, "Error marshalling item into json : %v", err)
	}

	return nil
}

func (m *UntypedPostgreSqlJsonDataStore) Get(ctx context.Context, key string) ([]byte, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sqlExists := fmt.Sprintf(`SELECT data FROM %v where ID=$1`, m.table)
	rows, err := conn.Query(ctx, sqlExists, key)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	defer rows.Close()
	if rows.Next() {
		var jsonResult []byte
		err = rows.Scan(&jsonResult)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error scaning into struct : %v", err)
		}
		return jsonResult, nil
	} else {
		return nil, nil
	}
}

func (m *UntypedPostgreSqlJsonDataStore) GetAll(ctx context.Context) ([][]byte, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sqlExists := fmt.Sprintf(`SELECT data FROM %v`, m.table)
	rows, err := conn.Query(ctx, sqlExists)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	var rtn [][]byte

	defer rows.Close()
	for rows.Next() {
		var jsonResult []byte
		err = rows.Scan(&jsonResult)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error scaning into struct : %v", err)
		}
		rtn = append(rtn, jsonResult)
	}

	return rtn, nil
}

func (m *UntypedPostgreSqlJsonDataStore) Delete(ctx context.Context, key string) error {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer m.returnConnection(ctx, conn)

	sqlDelete := fmt.Sprintf(`DELETE FROM %v where ID=$1`, m.table)
	_, err = conn.Exec(ctx, sqlDelete, key)
	if err != nil {
		return cloudy.Error(ctx, "Error deleteing %v from %v : %v", key, m.table, err)
	}
	return nil
}

func (m *UntypedPostgreSqlJsonDataStore) Exists(ctx context.Context, key string) (bool, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return false, err
	}
	defer m.returnConnection(ctx, conn)

	sqlExists := fmt.Sprintf(`SELECT ID FROM %v where ID=$1`, m.table)
	rows, err := conn.Query(ctx, sqlExists, key)
	if err != nil {
		return false, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	defer rows.Close()
	if rows.Next() {
		return true, nil
	} else {
		return false, nil
	}
}

func (m *UntypedPostgreSqlJsonDataStore) returnConnection(ctx context.Context, conn *pgxpool.Conn) {
	if conn == nil {
		fmt.Println("RETURNING NIL CONNECTION")
		return
	}

	// Dont get rid of the connection if it cam from the context
	obj := ctx.Value(m.ConnectionKey)
	if obj != nil && obj == conn {
		return
	}

	if m.provider != nil {
		m.provider.Return(ctx, conn)
	} else {
		conn.Release()
	}

}

func (m *UntypedPostgreSqlJsonDataStore) checkConnection(ctx context.Context) (*pgxpool.Conn, error) {
	var conn *pgxpool.Conn
	var err error

	// Check to see if there is a connection in the context. If not then add one
	obj := ctx.Value(m.ConnectionKey)
	if obj != nil {
		return obj.(*pgxpool.Conn), nil
	}

	if m.provider == nil {
		return nil, errors.New("no connection provider")
	}

	// // Check to see if there is a connection pool already created
	// if m.pool != nil {
	// 	conn, err = m.pool.Acquire(ctx)
	// 	return conn, err
	// }

	// config, err := pgxpool.ParseConfig(m.connectionString)
	// if err != nil {
	// 	return nil, cloudy.Error(ctx, "Unable to configure databsze: %v\n", err)
	// }

	// pool, err := pgxpool.ConnectConfig(ctx, config)
	// if err != nil {
	// 	return nil, cloudy.Error(ctx, "Unable to connect to database: %v\n", err)
	// }
	// m.pool = pool

	conn, err = m.provider.Acquire(ctx)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to aquire connection to database: %v\n", err)
	}

	// check that our table is there. If not then go ahead and add it
	sqlTableExists := fmt.Sprintf(`
		SELECT * 
		FROM information_schema.tables 
		WHERE 
		  table_schema = 'public' AND 
		  table_name = '%v';`, m.table)

	rows, err := conn.Query(ctx, sqlTableExists)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to query database: %v\n", err)
	}

	if rows.Next() {
		// Table exists!
		rows.Close()
	} else {
		rows.Close()

		// Table does not exist
		sqlTableCreate := fmt.Sprintf(`
			CREATE TABLE %v (
				id varchar(200) NOT NULL PRIMARY KEY, 
				data json NOT NULL
			);`, m.table)
		_, err := conn.Exec(ctx, sqlTableCreate)
		if err != nil {
			return nil, cloudy.Error(ctx, "Unable to create table: %v, %v\n", m.table, err)
		}

		// Load any Default Data
		if m.onCreate != nil {
			err := m.onCreate(ctx, m)
			if err != nil {
				return nil, cloudy.Error(ctx, "Unable to initialize table: %v, %v\n", m.table, err)
			}
		}

	}

	return conn, nil
}

func (m *UntypedPostgreSqlJsonDataStore) Query(ctx context.Context, query *datastore.SimpleQuery) ([][]byte, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, m.table)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	defer rows.Close()
	var rtn [][]byte
	for rows.Next() {
		var jsonResult []byte
		err = rows.Scan(&jsonResult)
		if err != nil {
			return rtn, err
		}
		rtn = append(rtn, jsonResult)
	}

	return rtn, nil
}

// Save an item to the MongoDB. This is implemented as an Upsert to this will work
// for new items as well as updates.
func (m *UntypedPostgreSqlJsonDataStore) QueryAndUpdate(ctx context.Context, query *datastore.SimpleQuery, updater func(ctx context.Context, items [][]byte) ([][]byte, error)) ([][]byte, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, m.table)

	var updated [][]byte
	// All this runs in a single transaction
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		sql = strings.Replace(sql, "SELECT", "SELECT FOR UPDATE", 1)
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			return err
		}

		defer rows.Close()
		var rtn [][]byte
		for rows.Next() {
			var jsonResult []byte
			err = rows.Scan(&jsonResult)
			if err != nil {
				return err
			}
			rtn = append(rtn, jsonResult)
		}

		updated, err = updater(ctx, rtn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// Save an item to the MongoDB. This is implemented as an Upsert to this will work
// for new items as well as updates.
func (m *UntypedPostgreSqlJsonDataStore) SaveAll(ctx context.Context, item [][]byte, key []string) error {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer m.returnConnection(ctx, conn)

	batch := &pgx.Batch{}
	for i := range key {
		sqlUpsert := fmt.Sprintf(`INSERT INTO %v (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2;`, m.table)
		batch.Queue(sqlUpsert, key[i], item[i])
	}

	br := conn.SendBatch(ctx, batch)
	_, err = br.Exec()
	if err != nil {
		return err
	}

	err = br.Close()
	if err != nil {
		return err
	}

	return nil
}

// Save an item to the MongoDB. This is implemented as an Upsert to this will work
// for new items as well as updates.
func (m *UntypedPostgreSqlJsonDataStore) DeleteAll(ctx context.Context, key []string) error {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer m.returnConnection(ctx, conn)

	batch := &pgx.Batch{}
	for i := range key {
		sqlDelete := fmt.Sprintf(`DELETE FROM %v where ID=$1`, m.table)
		batch.Queue(sqlDelete, key[i])
	}

	br := conn.SendBatch(ctx, batch)
	_, err = br.Exec()
	if err != nil {
		return err
	}
	err = br.Close()
	if err != nil {
		return err
	}
	return nil
}

func (m *UntypedPostgreSqlJsonDataStore) QueryAsMap(ctx context.Context, query *datastore.SimpleQuery) ([]map[string]any, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, m.table)

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}
	rtn, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (map[string]interface{}, error) {
		return pgx.RowToMap(row)
	})

	return rtn, nil
}

func (m *UntypedPostgreSqlJsonDataStore) QueryTable(ctx context.Context, query *datastore.SimpleQuery) ([][]interface{}, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, m.table)
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

type Column struct {
	Column string
	Label  string
	Data   interface{}
}
