package cloudypg

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/appliedres/cloudy"
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
	Image      string
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

type DedicatedPostgreSQLConnectionProvider struct {
	connstr string
	pool    *pgxpool.Pool
}

func NewDedicatedPostgreSQLConnectionProvider(connstr string) *DedicatedPostgreSQLConnectionProvider {
	return &DedicatedPostgreSQLConnectionProvider{
		connstr: connstr,
	}
}

func (db *DedicatedPostgreSQLConnectionProvider) Close(ctx context.Context) error {
	if db.pool != nil {
		db.pool.Close()
	}
	return nil
}

func (db *DedicatedPostgreSQLConnectionProvider) Connect(ctx context.Context, connstr string) error {
	if db.pool != nil {
		// Close in background
		go func() { db.pool.Close() }()
		db.pool = nil
	}

	pgconfig, err := pgxpool.ParseConfig(connstr)
	if err != nil {
		return cloudy.Error(ctx, "Unable to configure databsze: %v\n", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, pgconfig)
	if err != nil {
		return cloudy.Error(ctx, "Unable to connect to database: %v\n", err)
	}

	db.pool = pool
	return db.pool.Ping(ctx)
}

// Acquire A connection
func (db *DedicatedPostgreSQLConnectionProvider) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	if db.pool == nil && db.connstr == "" {
		return nil, errors.New("no connection pool configured")
	} else if db.pool == nil {
		err := db.Connect(ctx, db.connstr)
		if err != nil {
			return nil, err
		}
	}
	return db.pool.Acquire(ctx)
}

// Return a connection
func (db *DedicatedPostgreSQLConnectionProvider) Return(ctx context.Context, conn *pgxpool.Conn) {
	if conn != nil {
		conn.Release()
	}
}

func ConnectionString(host string, user string, password string, database string, port ...int) string {
	pgPort := 5432
	if len(port) == 1 {
		pgPort = port[0]
	}

	return fmt.Sprintf("postgres://%v:%v@%v:%v/%v",
		user,
		password,
		host,
		pgPort,
		database)
}

func Connect(ctx context.Context, cfg *PostgreSqlConfig) (*pgx.Conn, error) {
	connstr := ConnectionString(cfg.Host, cfg.User, cfg.Password, cfg.Database, int(cfg.Port))
	return pgx.Connect(ctx, connstr)
}
func ConnStringFrom(ctx context.Context, cfg *PostgreSqlConfig) string {
	return ConnectionString(cfg.Host, cfg.User, cfg.Password, cfg.Database, int(cfg.Port))
}
