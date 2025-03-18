package cloudypg

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"
)

/*
The PostgreSqlJsonDataStore is meant to be used with a single table. That table has the definition of:
ID: varchar2(30) PRIMARY KEY, DATA : jso
*/
type pgContextKey string

type PostgreSqlConfig struct {
	Table      string
	Connection string
	User       string
	Host       string
	Password   string
	Database   string
	Port       uint16
	// onCreateFn   func(ctx context.Context, ds datastore.JsonDataStore[T]) error
}

func (cfg *PostgreSqlConfig) GetConnectionString() string {
	if cfg.Connection != "" {
		return cfg.Connection
	}
	port := "5432"
	if cfg.Port != 0 {
		port = strconv.Itoa(int(cfg.Port))
	}

	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v", cfg.User, cfg.Password, cfg.Host, port, cfg.Database)
}

func ConfigFromMap(m map[string]interface{}) (*PostgreSqlConfig, error) {
	user, _ := cloudy.EnvKeyStr(m, "user")
	password, _ := cloudy.EnvKeyStr(m, "password")
	password = strings.TrimSpace(password)
	host, _ := cloudy.EnvKeyStr(m, "host")
	database, _ := cloudy.EnvKeyStr(m, "database")

	cfg := &PostgreSqlConfig{}
	cfg.User = user
	cfg.Password = password
	cfg.Host = host
	cfg.Database = database

	return cfg, nil
}

func ConfigFromEnv(env *cloudy.Environment) (*PostgreSqlConfig, error) {
	cfg := &PostgreSqlConfig{}
	cfg.User = env.Force("USER")
	cfg.Password = env.Force("PASSWORD")
	cfg.Password = strings.TrimSpace(cfg.Password)
	cfg.Host = env.Force("HOST")
	cfg.Database = env.Default("DATABASE", "postgres")

	return cfg, nil
}

func (cfg *PostgreSqlConfig) ForTable(table string) *PostgreSqlConfig {
	return &PostgreSqlConfig{
		Table:      table,
		User:       cfg.User,
		Password:   cfg.Password,
		Host:       cfg.Host,
		Database:   cfg.Database,
		Connection: cfg.Connection,
	}
}

type PostgreSqlJsonDataStore[T any] struct {
	connectionString string
	// client           *pgx.Conn
	Database      string
	table         string
	ConnectionKey pgContextKey
	pool          *pgxpool.Pool
	onCreateFn    func(ctx context.Context, ds datastore.JsonDataStore[T]) error
}

func NewPostgreSqlJsonDataStore[T any](ctx context.Context, config *PostgreSqlConfig) *PostgreSqlJsonDataStore[T] {
	// Generate Conne
	return &PostgreSqlJsonDataStore[T]{
		connectionString: config.GetConnectionString(),
		table:            config.Table,
		Database:         config.Database,
		// onCreateFn:         config.onCreateFn,

		ConnectionKey: pgContextKey(config.Table),
	}
}

func (m *PostgreSqlJsonDataStore[T]) Open(ctx context.Context, config interface{}) error {
	cloudy.Info(ctx, "Openning PostgreSqlJsonDataStore  %v", m.table)
	conn, err := m.checkConnection(ctx)
	m.returnConnection(ctx, conn)

	return err
}

func (m *PostgreSqlJsonDataStore[T]) OnCreate(fn func(ctx context.Context, ds datastore.JsonDataStore[T]) error) {
	m.onCreateFn = fn
}

// For Transactions and long operations with the datastore consider
// Creating a new context with the connection stored as an attribute
// This will keep the connection from being returned to the pool
// If you do this you must explicitly end the connection.
//
// TODO: Determine if this should be safe to call multiple times. If
//
//	so then we need a reference counter or something
func (m *PostgreSqlJsonDataStore[T]) NewConnectionContext(ctx context.Context) (context.Context, error) {
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

func (m *PostgreSqlJsonDataStore[T]) ReturnConnectionContext(ctx context.Context) {
	// Check to see if there is a connection in the context. If not then add one
	obj := ctx.Value(m.ConnectionKey)
	if obj != nil {
		conn := obj.(*pgxpool.Conn)
		conn.Release()
	}
}

func (m *PostgreSqlJsonDataStore[T]) Close(ctx context.Context) error {
	if m.pool != nil {
		m.pool.Close()
	}
	return nil
}

// Save an item to the MongoDB. This is implemented as an Upsert to this will work
// for new items as well as updates.
func (m *PostgreSqlJsonDataStore[T]) Save(ctx context.Context, item *T, key string) error {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return err
	}
	defer m.returnConnection(ctx, conn)

	modelJson, err := json.Marshal(item)
	if err != nil {
		return cloudy.Error(ctx, "Error marshalling item into json : %v", err)
	}

	// Not writing to cloudy to prevent build log duplication
	fmt.Printf("Writing to the database %s table %s: %v\n", m.Database, m.table, string(modelJson))

	sqlUpsert := fmt.Sprintf(`INSERT INTO %v (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2;`, m.table)
	_, err = conn.Exec(ctx, sqlUpsert, key, modelJson)
	if err != nil {
		return cloudy.Error(ctx, "Error writing data to the database : %v", err)
	}

	return nil
}

func (m *PostgreSqlJsonDataStore[T]) Get(ctx context.Context, key string) (*T, error) {
	// cloudy.Info(ctx, "PostgreSqlJsonDataStore.Get connecting")

	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	// cloudy.Info(ctx, "PostgreSqlJsonDataStore.Get checking data column from table: %s", m.table)

	sqlExists := fmt.Sprintf(`SELECT data FROM %v where ID=$1`, m.table)
	rows, err := conn.Query(ctx, sqlExists, key)
	if err != nil {
		_ = cloudy.Error(ctx, "PostgreSqlJsonDataStore.Get Error: db connection string: %s", SanitizeConnectionString(m.connectionString))
		_ = cloudy.Error(ctx, "PostgreSqlJsonDataStore.Get Error: SQL: %s", sqlExists)
		return nil, cloudy.Error(ctx, "PostgreSqlJsonDataStore.Get Error querying database : %v", err)
	}

	// cloudy.Info(ctx, "PostgreSqlJsonDataStore.Get fetching results")

	defer rows.Close()
	if rows.Next() {
		var instance T
		err = rows.Scan(&instance)
		if err != nil {
			return nil, cloudy.Error(ctx, "PostgreSqlJsonDataStore.Get Error scaning into struct : %v", err)
		}
		return &instance, nil
	} else {
		return nil, nil
	}
}

func (m *PostgreSqlJsonDataStore[T]) GetAll(ctx context.Context) ([]*T, error) {
	cloudy.Info(ctx, "PostgreSqlJsonDataStore.GetAll %s", m.table)

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

	var rtn []*T

	defer rows.Close()
	for rows.Next() {
		var instance T
		err = rows.Scan(&instance)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error scaning into struct : %v", err)
		}
		rtn = append(rtn, &instance)
	}

	if len(rtn) == 0 {
		cloudy.Info(ctx, "PostgreSqlJsonDataStore.GetAll %s is empty. Attempting to re-initialize", m.table)

		if m.onCreateFn != nil {
			err := m.onCreateFn(ctx, m)
			if err != nil {
				_ = cloudy.Error(ctx, "Unable to initialize table: %v, %v\n", m.table, err)
				return rtn, nil
			}

			newRtn, err := m.GetAll(ctx)
			if err != nil {
				_ = cloudy.Error(ctx, "Unable to getall for table: %v, %v\n", m.table, err)
				return rtn, nil
			}

			if len(newRtn) == 0 {
				cloudy.Info(ctx, "table %s is still empty", m.table)
				return rtn, nil
			}

			return newRtn, nil
		} else {
			cloudy.Info(ctx, "table %s has no oncreateFn function", m.table)
		}
	}

	return rtn, nil
}

func (m *PostgreSqlJsonDataStore[T]) Delete(ctx context.Context, key string) error {
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

func (m *PostgreSqlJsonDataStore[T]) Exists(ctx context.Context, key string) (bool, error) {
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

func (m *PostgreSqlJsonDataStore[T]) returnConnection(ctx context.Context, conn *pgxpool.Conn) {
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

func (m *PostgreSqlJsonDataStore[T]) Check(ctx context.Context) error {
	c, err := m.checkConnection(ctx)
	m.returnConnection(ctx, c)
	return err
}

func (m *PostgreSqlJsonDataStore[T]) checkConnection(ctx context.Context) (*pgxpool.Conn, error) {
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

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to connect to database: %v\n", err)
	}
	m.pool = pool

	conn, err = m.pool.Acquire(ctx)
	if err != nil {
		return nil, cloudy.Error(ctx, "Unable to aquire connection to database: %v\n", err)
	}

	// check that our table is there. If not then go ahead and add it
	tblName := strings.ToLower(m.table)
	sqlTableExists := fmt.Sprintf(`
		SELECT * 
		FROM information_schema.tables 
		WHERE 
		  table_schema = 'public' AND 
		  table_name = '%v';`, tblName)

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
		if m.onCreateFn != nil {
			err := m.onCreateFn(ctx, m)
			if err != nil {
				return nil, cloudy.Error(ctx, "Unable to initialize table: %v, %v\n", m.table, err)
			}
		}

	}

	return conn, nil
}

func (m *PostgreSqlJsonDataStore[T]) Query(ctx context.Context, query *datastore.SimpleQuery) ([]*T, error) {
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
	var rtn []*T
	for rows.Next() {
		var model T
		err = rows.Scan(&model)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error querying database : %v", err)
		}
		rtn = append(rtn, &model)
	}

	return rtn, nil
}

func (m *PostgreSqlJsonDataStore[T]) DeleteQuery(ctx context.Context, query *datastore.SimpleQuery) ([]string, error) {
	conn, err := m.checkConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer m.returnConnection(ctx, conn)

	sql := new(PgQueryConverter).ConvertDelete(query, m.table)

	// Execute the query
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("delete query failed: %w", err)
	}
	defer rows.Close()

	// Collect the returned IDs
	var deletedIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return deletedIDs, fmt.Errorf("error scanning row: %w", err)
		}
		deletedIDs = append(deletedIDs, id)
	}

	// Check for any errors encountered during iteration
	if err := rows.Err(); err != nil {
		return deletedIDs, fmt.Errorf("error iterating rows: %w", err)
	}

	return deletedIDs, nil
}
