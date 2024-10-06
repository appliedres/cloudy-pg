package cloudypg

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresqlConnectionProvider interface {
	// Acquire A connection
	Acquire(ctx context.Context) (*pgxpool.Conn, error)

	// Return a connection
	Return(ctx context.Context, conn *pgxpool.Conn)

	// Close Database
	Close(ctx context.Context) error
}

type PgxConnection struct {
	conn *pgxpool.Conn
}

// Acquire A connection
func (pgxc *PgxConnection) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return pgxc.conn, nil
}

// Return a connection
func (pgxc *PgxConnection) Return(ctx context.Context, conn *pgxpool.Conn) {

}

// Close Database
func (pgxc *PgxConnection) Close(ctx context.Context) error {
	return pgxc.conn.Conn().Close(ctx)
}
