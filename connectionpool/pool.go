package connectionpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrClosed is returned then Close() is called while the ConnectionPool is closed.
var ErrClosed = errors.New("ConnectionPool already closed")

// ConnectionPool is a SQL connection pool which supports multiple users. Normally, connection pools
// have a single configuration which defines static credentials to use when opening a new
// connection, but this does not satisfy all use cases. The usecase for supporting multiple users is
// to provide a proxy server on top of a SQL database which allows different users to login
// while still maintaining control over the load exerted on the underlying system.
type ConnectionPool struct {
	host    string
	port    string
	closed  bool
	manager *ConnectionManager
}

// Auth is the information used to authenticate to the database.
type Auth struct {
	Username string
	Password string
}

// New returns a new connection pool.
func New(host, port string, connectionLimit int, idleTimeout time.Duration) *ConnectionPool {
	manager := newConnectionManager(host, port, connectionLimit, idleTimeout, false, nil)
	return &ConnectionPool{
		host:    host,
		port:    port,
		manager: manager,
	}
}

// newForTesting passes an extra connReqChan parameter so the calling test can redirect requests.
func newForTesting(
	host, port string, connectionLimit int, idleTimeout time.Duration, connReqChan chan connectionRequest,
) *ConnectionPool {
	manager := newConnectionManager(host, port, connectionLimit, idleTimeout, true, connReqChan)
	return &ConnectionPool{
		host:    host,
		port:    port,
		manager: manager,
	}
}

// Query executs a query against the given database with given authentication parameters.
func (c *ConnectionPool) Query(ctx context.Context, auth Auth, database, sql string) (*sql.Rows, error) {
	if c.closed {
		return nil, fmt.Errorf("ConnectionPool is closed")
	}
	conn, err := c.manager.requestConnection(ctx, auth, database)
	defer c.manager.returnConnection(ctx, conn, err)
	if err != nil {
		return nil, err
	}
	r, err := conn.Connection.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Exec executs a query returning now rows against the given database with given authentication
// parameters.
func (c *ConnectionPool) Exec(ctx context.Context, auth Auth, database, sql string) (sql.Result, error) {
	if c.closed {
		return nil, fmt.Errorf("ConnectionPool is closed")
	}
	conn, err := c.manager.requestConnection(ctx, auth, database)
	defer c.manager.returnConnection(ctx, conn, err)
	if err != nil {
		return nil, err
	}
	r, err := conn.Connection.ExecContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Close closes the ConnectionPool. After Close is called the ConnectionPool cannot be used.
func (c *ConnectionPool) Close() error {
	if c.closed {
		return ErrClosed
	}
	c.closed = true
	c.manager.close()
	<-c.manager.done()
	return c.manager.error()
}
