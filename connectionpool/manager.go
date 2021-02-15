package connectionpool

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aastein/singlestore/orderedmap"
)

//go:generate ../bin/mockgen -destination=mock_sql_connection.go -package connectionpool -source $PWD/connectionpool/manager.go sqlConnection

// ConnectionManager is responsible for receiving and handling requests for open database
// connections scoped to a specific database and specific credentials.
type ConnectionManager struct {
	host string
	port string
	// connectionLimit is the total number of connections that can be open at any time
	connectionLimit int
	// idleTimeout is the duration which a connection can remain idle before being closed
	idleTimeout time.Duration

	// used for debugging
	lg *log.Logger

	// used to coordinate closing the connection pool/manager
	ctx    context.Context
	cancel context.CancelFunc
	// signals to the caller of Close() that closing has completed
	doneCh chan struct{}
	// any error encountered while closing
	closeErr error

	// connectionpool requests a connection
	connReqChan chan connectionRequest
	// connectionmanager receives a used connection from connectionpool
	connReceiveChan chan *connectionReceive

	// idleConnections is an ordered map containing lists of idle connections keyed by their
	// connection configuration
	idleConnections  *orderedmap.OrderedMap
	totalConnections int
	// totalIdleConnections is the total number of idle connections contained in idleConnections
	totalIdleConnections int
	// pendingRequests is a list of requests which could not immediately receive a connection
	pendingRequests []*connectionRequest
}

// connectionConfig is the unique configration for a connection.
type connectionConfig struct {
	auth     Auth
	database string
}

type sqlOpenFunc func(driverName, dataSourceName string) (*sql.DB, error)
type sqlConnection interface {
	Close() error
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// connection contains a connection to a SQL database.
type connection struct {
	Connection sqlConnection
	config     connectionConfig
	// idleStartTime is the time the connection was added to the idle connection map.
	idleStartTime time.Time
}

// close closes the underlying database connection of a connection.
func (c *connection) close() error {
	return c.Connection.Close()
}

// connectionRequest is sent by ConnectionPool to request a database connection with the given
// connection configuration.
type connectionRequest struct {
	config   connectionConfig
	connChan chan *connectionResponse
	ctx      context.Context
}

// connectionResponse is sent by ConnectionManager to the ConnectionPool. It contains a connection
// or an error that was encountered while trying to get a connection.
type connectionResponse struct {
	conn *connection
	err  error
}

// connectionReceive is sent by the ConnectionPool to return a no longer used connection.s
type connectionReceive struct {
	conn *connection
	ctx  context.Context
	err  error
}

type contextKey string

// ContextRequestID is the field used to key a request ID which is stored in a context.Context.
const ContextRequestID = contextKey("requestID")

// NewConnectionManager returns a new ConnectionManager. The ConnectionManager is responsible for
// receiving requests
// for open connections to a database and handling them appropriately. The debug parameter can be
// set to true in pool.go to enable debug logging.
// connReqChan argument is used by test to redirect requests that are sent to the connection
// manager.
func newConnectionManager(host, port string, connectionLimit int, idleTimeout time.Duration,
	debug bool, testReqChan chan connectionRequest) *ConnectionManager {
	ctx, cancel := context.WithCancel(context.Background())
	var lg *log.Logger
	if debug {
		lg = log.New(os.Stdout, "ConnectionManager - ", log.Lmicroseconds|log.Lshortfile)
	}
	manager := &ConnectionManager{
		host:            host,
		port:            port,
		lg:              lg,
		ctx:             ctx,
		cancel:          cancel,
		doneCh:          make(chan struct{}),
		connectionLimit: connectionLimit,
		idleTimeout:     idleTimeout,
		connReceiveChan: make(chan *connectionReceive),
		idleConnections: orderedmap.New(),
	}

	// If testReqChan is empty, the connection manager and run loop will get the same connection
	// request channel. For testing purposes the testReqChan can be specified so connection requests
	// are sent to the testReqChan and the run loop never receives connection requests.
	if testReqChan == nil {
		connReqChan := make(chan connectionRequest)
		manager.connReqChan = connReqChan
		go manager.run(connReqChan)
	} else {
		manager.connReqChan = testReqChan
		go manager.run(make(chan connectionRequest))
	}

	return manager
}

// requestConnection sends a request for an open database connection to ConnectionManager and waits
// for ConnectionManager to return a valid connection.
func (c *ConnectionManager) requestConnection(
	ctx context.Context, auth Auth, database string,
) (*connection, error) {

	connRespChan := make(chan *connectionResponse)

	select {
	case <-c.ctx.Done():
		c.debug(ctx, "requestConnection: abort request because shutdown")
		return nil, c.ctx.Err()
	case <-ctx.Done():
		c.debug(ctx, "requestConnection: abort request because caller context is done")
		return nil, ctx.Err()
	case c.connReqChan <- connectionRequest{
		config: connectionConfig{
			auth:     auth,
			database: database,
		},
		ctx:      ctx,
		connChan: connRespChan,
	}:
	}

	c.debug(ctx, "requestConnection:: waiting for connection")
	select {
	case <-c.ctx.Done():
		c.debug(ctx, "requestConnection: abort waiting because shutdown")
		return nil, fmt.Errorf("connection pool shutting down: %w", c.ctx.Err())
	case <-ctx.Done():
		c.debug(ctx, "requestConnection: aborted waiting because caller context is done")
		return nil, fmt.Errorf("context sent with request is done: %w", ctx.Err())
	case resp := <-connRespChan:
		c.debug(ctx, "requestConnection: received connection")
		return resp.conn, resp.err
	}
}

// returnConnection sends a used connection back to the connection manager.
func (c *ConnectionManager) returnConnection(
	ctx context.Context, conn *connection, err error,
) error {
	if conn == nil {
		// caller never received a connection to return
		return nil
	}
	item := &connectionReceive{
		conn: conn,
		ctx:  ctx,
		err:  err,
	}

	select {
	case <-c.ctx.Done():
		if err := conn.close(); err != nil {
			return fmt.Errorf("error closing connection after connection pool closed: %w", err)
		}
		return c.ctx.Err()
	case c.connReceiveChan <- item:
		return nil
	}
}

// close signals that the ConnectionManager should close all idle connections and not serve any
// new requests.
func (c *ConnectionManager) close() {
	c.cancel()
}

// done signals that all work needed to close the ConnectionManager has completed.
func (c *ConnectionManager) done() chan struct{} {
	return c.doneCh
}

// error should be called after the done channel is closed, similar to context.Error(). It returns
// an error encountered while closign ConnectionManager if an error was encountered.
func (c *ConnectionManager) error() error {
	return c.closeErr
}

// run is the main loop of ConnectionManager. It serves to ensure that a ConnectionManager's
// resources are only accessed by a single routine at a time. The connection request channel
// is passed as an argument to tests can intercept connection requests.
func (c *ConnectionManager) run(connReqChan chan connectionRequest) {
	for {
		select {
		case <-c.ctx.Done():
			if err := c.closeIdleConnections(c.ctx); err != nil {
				c.closeErr = err
			}
			close(c.doneCh)
			return
		case req := <-connReqChan:
			c.debug(req.ctx, "run: received connection request")
			c.handleConnectionRequest(&req, sql.Open)
		case resp := <-c.connReceiveChan:
			c.debug(resp.ctx, "run: received connection received")
			c.handleConnectionReceive(resp, sql.Open)
		}
	}
}

// handleConnectionRequest tries to return an open connection which matches the configuration of the
// given connectionRequest. If no connection can be sent to the requester the request is added to
// the ConnectionManager's list of pending requests.
func (c *ConnectionManager) handleConnectionRequest(r *connectionRequest, fn sqlOpenFunc) {
	conn, err := c.tryGetConnection(r.ctx, r.config, fn)
	if err != nil {
		c.debug(r.ctx, "handleConnectionRequest: send connection request error: %v", err)
		r.connChan <- &connectionResponse{
			conn: nil,
			err:  err,
		}
		return
	}
	if conn != nil {
		c.debug(r.ctx, "handleConnectionRequest: sending connection")
		select {
		case <-r.ctx.Done():
			c.appendToIdleConnections(conn)
			c.debug(r.ctx, "handleConnectionRequest: abort send connection, requester cotext done")
		case r.connChan <- &connectionResponse{
			conn: conn,
			err:  err,
		}:
			c.debug(r.ctx, "handleConnectionRequest: sent connection")
		}
		return
	}
	// If the connection pool is full and has no idle connections then block until an
	// idle connection becomes available to either reuse or replace
	c.addRequestToPendingRequests(r)
}

// handleConnectionReceive adds the received connetion back to connection pool then tries to
// fulfill any pending requests.
func (c *ConnectionManager) handleConnectionReceive(rec *connectionReceive, fn sqlOpenFunc) error {
	if rec.err != nil {
		// TODO: the error value of rec is unused, but could be used to determine if to cancel the
		// connection or return it to the pool
		if err := c.closeConnection(rec.conn); err != nil {
			return err
		}
	} else {
		c.appendToIdleConnections(rec.conn)
	}
	c.pruneExpiredConnections(rec)
	return c.fulfillPendingRequests(rec.ctx, fn)
}

// addRequestToPendingRequests adds a connection request to the list of pending requests.
func (c *ConnectionManager) addRequestToPendingRequests(request *connectionRequest) {
	c.debug(request.ctx, "addRequestToPendingRequests: add to pending requests")
	c.pendingRequests = append(c.pendingRequests, request)
}

// tryGetConnection tries to return an open connection.
func (c *ConnectionManager) tryGetConnection(
	ctx context.Context, config connectionConfig, fn sqlOpenFunc,
) (*connection, error) {
	// If an idle connection matching the requested connection details is available, use
	// it to run the query.
	if v, ok := c.idleConnections.Get(config); ok {
		conns := v.([]*connection)
		conn := conns[0]
		conns = conns[1:]
		c.setIdleConnections(config, conns, -1)
		return conn, nil
	}

	// If the connection pool is not yet full, open a new connection as needed.
	// Assumes a connectionlimit of 0 or less means to connection limit.
	if c.connectionLimit <= 0 || c.totalConnections < c.connectionLimit {
		conn, err := c.openConnection(ctx, config, fn)
		return conn, err
	}

	// TODO
	// If the connection pool is full, but has idle connections associated with other
	// connection details, close an idle connection associated with the least recently
	// used connection details and return a new connection.
	if c.idleConnections.Len() > 0 {
		front := c.idleConnections.Front()
		conns := front.Value.([]*connection)
		leastRecentlyUsedConn := conns[0]
		conns = conns[1:]
		if err := leastRecentlyUsedConn.close(); err != nil {
			return nil, err
		}
		c.setIdleConnections(front.Key, conns, -1)
		conn, err := c.openConnection(ctx, config, fn)
		return conn, err

	}
	return nil, nil
}

// setIdleConnections sets an entry in the idle connection map while ensuring no entires in
// the idle connection map contain empty lists.
// todo: improve this so there is just (key, oldConns, newConns)
func (c *ConnectionManager) setIdleConnections(key interface{}, connections []*connection, incr int) {
	if len(connections) > 0 {
		c.idleConnections.Set(key, connections)
	} else {
		c.idleConnections.Delete(key)
	}
	c.totalIdleConnections += incr
}

// pruneExpiredConnections closed connections which have been idle for longer then the idle timeout
// and removes them from the idle connections map.
func (c *ConnectionManager) pruneExpiredConnections(rec *connectionReceive) {
	if c.idleTimeout > 0 {
		c.debug(rec.ctx, "pruneExpiredConnections: prune connections")
		e := c.idleConnections.Front()
		expireTime := time.Now().Add(-c.idleTimeout)
		toClose := []*connection{}
		for e != nil {
			conns := e.Value.([]*connection)
			next := e.Next()
			freshConns := []*connection{}
			removed := 0
			for _, conn := range conns {
				if conn.idleStartTime.Add(c.idleTimeout).Before(expireTime) {
					toClose = append(toClose, conn)
					removed++
				} else {
					freshConns = append(freshConns, conn)
				}
			}
			c.setIdleConnections(e.Key, freshConns, -removed)
			e = next
		}
		if len(toClose) > 0 {
			c.debug(rec.ctx, "pruneExpiredConnections: closing expired conns: %v remaining: %v",
				len(toClose), c.idleConnections.Len())
			c.closeConnections(toClose)
		}
	}
}

// fulfillPendingRequests tries to fulfill as many pending requests as possible by getting a
// connection for each request and sending the connection over the request's connection channel.
func (c *ConnectionManager) fulfillPendingRequests(ctx context.Context, fnOpenConn sqlOpenFunc) error {
	c.debug(ctx, "fulfillPendingRequests: try fulfill pending requests")
	for len(c.pendingRequests) == 0 {
		c.debug(ctx, "fulfillPendingRequests: no pending requests")
		return nil
	}

	totalRequests := len(c.pendingRequests)
	maxConnsSendable := totalRequests
	if c.connectionLimit > 0 {
		maxConnsSendable = c.connectionLimit - (c.totalConnections - c.totalIdleConnections)
		if maxConnsSendable > totalRequests {
			maxConnsSendable = totalRequests
		}
	}

	requestsToFulfill := c.pendingRequests[:maxConnsSendable]
	c.pendingRequests = c.pendingRequests[maxConnsSendable:]
	c.debug(ctx, "fulfillPendingRequests: fullfill requests: %v remaining: %v",
		len(requestsToFulfill), len(c.pendingRequests))
	for _, request := range requestsToFulfill {
		conn, err := c.tryGetConnection(request.ctx, request.config, fnOpenConn)
		if err != nil {
			return err
		}
		if conn != nil {
			c.debug(request.ctx, "fulfillPendingRequests: sending connection")
			select {
			case <-request.ctx.Done():
				c.appendToIdleConnections(conn)
				c.debug(request.ctx,
					"fulfillPendingRequests: aborted sending connection, request context done")
			case request.connChan <- &connectionResponse{
				conn: conn,
			}:
				c.debug(request.ctx, "fulfillPendingRequests: sent connection")
			}
		}
		if conn == nil {
			// The length of requestsToFulfill should be equal to the amount of connections that are
			// idle plus the difference between the connection limit and the current number of
			// connections. If this condition is hit there is a contradiction between this method
			// and tryGetConnection.
			panic("unexpectedly could not get connection for request.")
		}
	}
	return nil
}

// closeConnections closes a list of connections, ignoring any errors.
func (c *ConnectionManager) closeConnections(conns []*connection) {
	for _, conn := range conns {
		c.closeConnection(conn)
	}
}

// closeIdleConnections closes all idle connections. It is called during the close procedure. If
// any errors are encountered while closing connections, the last returned error is returned to
// the caller.
func (c *ConnectionManager) closeIdleConnections(ctx context.Context) error {
	c.debug(ctx, "closeIdleConnections: closeing")
	var closeErr error
	e := c.idleConnections.Front()
	for e != nil {
		next := e.Next()
		conns := e.Value.([]*connection)
		for _, conn := range conns {
			if err := c.closeConnection(conn); err != nil {
				closeErr = err
			}
			c.totalIdleConnections--
		}
		e = next
	}
	if closeErr != nil {
		return fmt.Errorf("closeIdleConnections one or more errors: %v", closeErr)
	}
	return nil
}

// appendToIdleConnections appens a connection the the list of idle connections in the idle
// connections map.
func (c *ConnectionManager) appendToIdleConnections(conn *connection) {
	var conns []*connection
	conn.idleStartTime = time.Now()
	if cs, ok := c.idleConnections.Get(conn.config); ok {
		conns = cs.([]*connection)
		conns = append(conns, conn)
	} else {
		conns = []*connection{}
		conns = append(conns, conn)
	}
	c.setIdleConnections(conn.config, conns, 1)
}

// openConnection opens a new connection to a database.
func (c *ConnectionManager) openConnection(
	ctx context.Context, config connectionConfig, fn sqlOpenFunc,
) (*connection, error) {
	c.debug(ctx, "openConnection: opening connection")
	var b strings.Builder
	fmt.Fprintf(&b, "%s:%s@tcp(%s:%s)/%s",
		config.auth.Username, config.auth.Username, c.host, c.port, config.database)
	db, err := fn("mysql", b.String())
	if err != nil {
		return nil, err
	}
	c.totalConnections++
	return &connection{
		Connection: db,
		config:     config,
	}, nil
}

// closeConnection closes a connection. If successful decrements the amount of total connections.
// If closing the connection returns an error, assume the connection is open and unusable.
func (c *ConnectionManager) closeConnection(conn *connection) error {
	if err := conn.close(); err != nil {
		return err
	}
	c.totalConnections--
	return nil
}

// requestID returns the request ID contained in a context.
func requestID(ctx context.Context) interface{} {
	return ctx.Value(ContextRequestID)
}

func (c *ConnectionManager) debug(ctx context.Context, format string, args ...interface{}) {
	if c.lg != nil {
		sb := strings.Builder{}
		if ctx != nil {
			sb.WriteString(fmt.Sprint(requestID(ctx)))
			sb.WriteString("::")
		}
		sb.WriteString(fmt.Sprintf(format, args...))
		c.lg.Print(sb.String())
	}
}
