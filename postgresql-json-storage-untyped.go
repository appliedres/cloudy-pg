package cloudypg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"

	"github.com/jackc/pgx/v4/pgxpool"
)

// const PostgresProviderID = "postgresql"

// func init() {
// 	datastore.JsonDataStoreProviders.Register(PostgresProviderID, &PostgreSqlJsonDataStoreFactory{})
// }

// type PostgreSqlJsonDataStoreFactory struct{}

// func (c *PostgreSqlJsonDataStoreFactory) Create(cfg interface{}) (datastore.UntypedJsonDataStore, error) {
// 	config := cfg.(*PostgreSqlConfig)
// 	return &UntypedPostgreSqlJsonDataStore{
// 		connectionString: config.GetConnectionString(),
// 		table:            config.Table,
// 		Database:         config.Database,
// 		// OnCreate:         config.OnCreate,

// 		ConnectionKey: pgContextKey(config.Table),
// 	}, nil
// }

// func (c *PostgreSqlJsonDataStoreFactory) FromEnv(env *cloudy.SegmentedEnvironment) (interface{}, error) {
// 	var found bool
// 	cfg := &PostgreSqlConfig{}

// 	cfg.Connection, _ = cloudy.MapKeyStr(config, "Connection", true)
// 	cfg.User, _ = cloudy.MapKeyStr(config, "User", true)
// 	cfg.Host, _ = cloudy.MapKeyStr(config, "Host", true)
// 	cfg.Password, _ = cloudy.MapKeyStr(config, "Password", true)
// 	cfg.Database, _ = cloudy.MapKeyStr(config, "Database", true)

// 	// Check that either Connection or (user,Host,Pass, database) is present
// 	if cfg.Connection != "" || (cfg.User != "" && cfg.Host != "" && cfg.Password != "" && cfg.Database != "") {
// 		return nil, errors.New("connection or User,Host,Password,Database must be specified")
// 	}

// 	cfg.Table, found = cloudy.MapKeyStr(config, "Table", true)
// 	if !found {
// 		return nil, errors.New("table required")
// 	}

// 	return cfg, nil
// }

type UntypedPostgreSqlConfig struct {
	PostgreSqlConfig
	Model interface{}
	// OnCreate   func(ctx context.Context, ds datastore.JsonDataStore) error
}

type UntypedPostgreSqlJsonDataStore struct {
	model            interface{}
	connectionString string
	// client           *pgx.Conn
	Database      string
	table         string
	ConnectionKey pgContextKey
	pool          *pgxpool.Pool
	OnCreate      func(ctx context.Context, ds *UntypedPostgreSqlJsonDataStore) error
	Model         interface{}
}

func NewUntypedPostgreSqlJsonDataStore(ctx context.Context, config *UntypedPostgreSqlConfig) *UntypedPostgreSqlJsonDataStore {
	// Generate Conne
	return &UntypedPostgreSqlJsonDataStore{
		connectionString: config.GetConnectionString(),
		table:            config.Table,
		Database:         config.Database,
		Model:            config.Model,
		// OnCreate:         config.OnCreate,

		ConnectionKey: pgContextKey(config.Table),
	}
}

//SAME
func (m *UntypedPostgreSqlJsonDataStore) Open(ctx context.Context, config interface{}) error {
	cloudy.Info(ctx, "Openning Postgres %v", m.table)
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
//       so then we need a reference counter or something
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
	if m.pool != nil {
		m.pool.Close()
	}
	return nil
}

// Save an item to the MongoDB. This is implemented as an Upsert to this will work
// for new items as well as updates.
func (m *UntypedPostgreSqlJsonDataStore) Save(ctx context.Context, item interface{}, key string) error {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer m.returnConnection(ctx, conn)

	modelJson, err := json.Marshal(item)
	if err != nil {
		return cloudy.Error(ctx, "Error marshalling item into json : %v", err)
	}

	sqlUpsert := fmt.Sprintf(`INSERT INTO %v (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2;`, m.table)
	_, err = conn.Exec(ctx, sqlUpsert, key, modelJson)
	if err != nil {
		return cloudy.Error(ctx, "Error marshalling item into json : %v", err)
	}

	return nil
}

func (m *UntypedPostgreSqlJsonDataStore) Get(ctx context.Context, key string) (interface{}, error) {
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
		instance := cloudy.NewInstance(m.Model)
		err = rows.Scan(&instance)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error scaning into struct : %v", err)
		}
		return &instance, nil
	} else {
		return nil, nil
	}
}

func (m *UntypedPostgreSqlJsonDataStore) GetAll(ctx context.Context) ([]interface{}, error) {
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

	var rtn []interface{}

	defer rows.Close()
	for rows.Next() {
		instance := cloudy.NewInstance(m.Model)
		err = rows.Scan(&instance)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error scaning into struct : %v", err)
		}
		rtn = append(rtn, &instance)
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

	conn.Release()
}

func (m *UntypedPostgreSqlJsonDataStore) checkConnection(ctx context.Context) (*pgxpool.Conn, error) {
	var conn *pgxpool.Conn
	var err error

	// Check to see if there is a connection in the context. If not then add one
	obj := ctx.Value(m.ConnectionKey)
	if obj != nil {
		return obj.(*pgxpool.Conn), nil
	}

	// Check to see if there is a connection pool already created
	if m.pool != nil {
		conn, err = m.pool.Acquire(ctx)
		return conn, err
	}

	config, err := pgxpool.ParseConfig(m.connectionString)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to configure databsze: %v\n", err)
	}

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to connect to database: %v\n", err)
	}
	m.pool = pool

	conn, err = m.pool.Acquire(ctx)
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
		if m.OnCreate != nil {
			err := m.OnCreate(ctx, m)
			if err != nil {
				return nil, cloudy.Error(ctx, "Unable to initialize table: %v, %v\n", m.table, err)
			}
		}

	}

	return conn, nil
}

func (m *UntypedPostgreSqlJsonDataStore) Query(ctx context.Context, query *datastore.SimpleQuery) ([]interface{}, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).Convert(query, m.table)
	fmt.Println(sql)
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, cloudy.Error(ctx, "Error querying database : %v", err)
	}

	defer rows.Close()
	var rtn []interface{}
	for rows.Next() {
		model := cloudy.NewInstance(m.Model)
		rows.Scan(&model)
		rtn = append(rtn, &model)
	}

	return rtn, nil
}
