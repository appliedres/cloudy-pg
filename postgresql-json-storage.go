package cloudypg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/appliedres/cloudy"
	"github.com/appliedres/cloudy/datastore"

	"github.com/jackc/pgx/v4/pgxpool"
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
	// OnCreate   func(ctx context.Context, ds datastore.JsonDataStore[T]) error
}

func (cfg *PostgreSqlConfig) GetConnectionString() string {
	if cfg.Connection != "" {
		return cfg.Connection
	}
	return fmt.Sprintf("postgres://%v:%v@%v:5432/%v", cfg.User, cfg.Password, cfg.Host, cfg.Database)
}

func ConfigFromMap(m map[string]interface{}) (*PostgreSqlConfig, error) {
	user, _ := cloudy.EnvKeyStr(m, "user")
	password, _ := cloudy.EnvKeyStr(m, "password")
	host, _ := cloudy.EnvKeyStr(m, "host")
	database, _ := cloudy.EnvKeyStr(m, "database")

	cfg := &PostgreSqlConfig{}
	cfg.User = user
	cfg.Password = password
	cfg.Host = host
	cfg.Database = database

	return cfg, nil
}

func ConfigFromEnv(env *cloudy.SegmentedEnvironment) (*PostgreSqlConfig, error) {
	cfg := &PostgreSqlConfig{}
	cfg.User = env.Force("USER")
	cfg.Password = env.Force("PASSWORD")
	cfg.Host = env.Force("HOST")
	cfg.Database, _ = env.Default("DATABASE", "postgres")

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
	OnCreate      func(ctx context.Context, ds *PostgreSqlJsonDataStore[T]) error
}

func NewPostgreSqlJsonDataStore[T any](ctx context.Context, config *PostgreSqlConfig) *PostgreSqlJsonDataStore[T] {
	// Generate Conne
	return &PostgreSqlJsonDataStore[T]{
		connectionString: config.GetConnectionString(),
		table:            config.Table,
		Database:         config.Database,
		// OnCreate:         config.OnCreate,

		ConnectionKey: pgContextKey(config.Table),
	}
}

func (m *PostgreSqlJsonDataStore[T]) Open(ctx context.Context, config interface{}) error {
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

	sqlUpsert := fmt.Sprintf(`INSERT INTO %v (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data=$2;`, m.table)
	_, err = conn.Exec(ctx, sqlUpsert, key, modelJson)
	if err != nil {
		return cloudy.Error(ctx, "Error marshalling item into json : %v", err)
	}

	return nil
}

func (m *PostgreSqlJsonDataStore[T]) Get(ctx context.Context, key string) (*T, error) {
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
		var instance T
		err = rows.Scan(&instance)
		if err != nil {
			return nil, cloudy.Error(ctx, "Error scaning into struct : %v", err)
		}
		return &instance, nil
	} else {
		return nil, nil
	}
}

func (m *PostgreSqlJsonDataStore[T]) GetAll(ctx context.Context) ([]*T, error) {
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
		rows.Scan(&model)
		rtn = append(rtn, &model)
	}

	return rtn, nil
}

type PgQueryConverter struct {
}

func (qc *PgQueryConverter) Convert(c *datastore.SimpleQuery, table string) string {
	where := qc.ConvertConditionGroup(c.Conditions)
	sort := qc.ConvertSort(c.SortBy)

	sql := qc.ConvertSelect(c, table)
	if where != "" {
		sql = sql + " WHERE " + where
	}
	if sort != "" {
		sql = sql + " ORDER BY " + sort
	}
	return sql
}

func (qc *PgQueryConverter) ConvertSelect(c *datastore.SimpleQuery, table string) string {
	str := "SELECT "
	if len(c.Colums) == 0 {
		str += " data"
	} else {
		var jsonQuery []string
		for _, col := range c.Colums {
			jsonQuery = append(jsonQuery, fmt.Sprintf("data ->> '%v' as \"%v\"", col, col))
		}
		str += strings.Join(jsonQuery, ", ")
	}

	if c.Size > 0 {
		str = fmt.Sprintf("%v LIMIT %v", str, c.Size)
	}

	if c.Offset > 0 {
		str = fmt.Sprintf("%v OFFSET %v", str, c.Offset)
	}

	str += " FROM " + table
	return str
}

func (qc *PgQueryConverter) ConvertSort(sortbys []*datastore.SortBy) string {
	if len(sortbys) == 0 {
		return ""
	}
	var sorts []string
	for _, sortBy := range sortbys {
		sort := qc.ConvertASort(sortBy)
		if sort != "" {
			sorts = append(sorts, sort)
		}
	}
	return strings.Join(sorts, ", ")
}
func (qc *PgQueryConverter) ConvertASort(c *datastore.SortBy) string {
	if c.Descending {
		return c.Field + " DESC"
	} else {
		return c.Field + "ASC"
	}
}

func (qc *PgQueryConverter) ConvertCondition(c *datastore.SimpleQueryCondition) string {
	switch c.Type {
	case "eq":
		return fmt.Sprintf("(data->>'%v') = '%v'", c.Data[0], c.Data[1])
	case "neq":
		return fmt.Sprintf("(data->>'%v') != '%v'", c.Data[0], c.Data[1])
	case "between":
		return fmt.Sprintf("(data->>'%v')::numeric BETWEEN %v AND %v", c.Data[0], c.Data[1], c.Data[2])
	case "lt":
		return fmt.Sprintf("(data->>'%v')::numeric < %v", c.Data[0], c.Data[1])
	case "lte":
		return fmt.Sprintf("(data->>'%v')::numeric  <= %v", c.Data[0], c.Data[1])
	case "gt":
		return fmt.Sprintf("(data->>'%v')::numeric  > %v", c.Data[0], c.Data[1])
	case "gte":
		return fmt.Sprintf("(data->>'%v')::numeric  >= %v", c.Data[0], c.Data[1])
	case "before":
		val := c.GetDate("value")
		if !val.IsZero() {
			timestr := val.UTC().Format(time.RFC3339)
			// return fmt.Sprintf("(data->'%v')::timestamptz < '%v'", c.Data[0], timestr)
			return fmt.Sprintf("to_date((data->>'%v'), 'YYYY-MM-DDTHH24:MI:SS.MSZ') < '%v'", c.Data[0], timestr)
		}
	case "after":
		val := c.GetDate("value")
		if !val.IsZero() {
			timestr := val.UTC().Format(time.RFC3339)
			// return fmt.Sprintf("(data->'%v')::timestamptz > '%v'", c.Data[0], timestr)
			return fmt.Sprintf("to_date((data->>'%v'), 'YYYY-MM-DDTHH24:MI:SS.MSZ') > '%v'", c.Data[0], timestr)
		}
	case "?":
		return fmt.Sprintf("(data->>'%v')::numeric  ? '%v'", c.Data[0], c.Data[1])
	case "contains":
		return fmt.Sprintf("(data->'%v')::jsonb ? '%v'", c.Data[0], c.Data[1])
	case "includes":
		values := c.GetStringArr("value")
		var xformed []string
		for _, v := range values {
			xformed = append(xformed, fmt.Sprintf("'%v'", v))
		}
		if values != nil {
			return fmt.Sprintf("(data->>'%v') in (%v)", c.Data[0], strings.Join(xformed, ","))
		}
	}
	return "UNKNOWN"
}

func (qc *PgQueryConverter) ConvertConditionGroup(cg *datastore.SimpleQueryConditionGroup) string {
	if len(cg.Conditions) == 0 && len(cg.Groups) == 0 {
		return ""
	}

	var conditionStr []string
	for _, c := range cg.Conditions {
		conditionStr = append(conditionStr, qc.ConvertCondition(c))
	}
	for _, c := range cg.Groups {
		result := qc.ConvertConditionGroup(c)
		if result != "" {
			conditionStr = append(conditionStr, "( "+result+" )")
		}
	}
	return strings.Join(conditionStr, " "+cg.Operator+" ")
}

func (qc *PgQueryConverter) ToColumnName(name string) string {
	return "data ->> " + name
}
