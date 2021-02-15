package connectionpool

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/aastein/singlestore/orderedmap"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestConnectionPool_Query(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T, *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string)
		validate func(*testing.T, *ConnectionPool)
		want     *sql.Rows
		wantErr  bool
	}{
		{
			name: "get connection and query",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx := context.Background()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().QueryContext(ctx, query).Return(nil, nil).Times(1)
				// validates that connection pool is not empty at end of test
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				pool := newForTesting("", "", 1, 0, nil)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {
				require.NoError(t, c.Close())
			},
			want: nil,
		},
		{
			name: "get connection error: shutdown when sending request",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx := context.Background()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				pool := newForTesting("", "", 1, 0, nil)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				err := pool.Close()
				require.NoError(t, err)
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {},
			want:     nil,
			wantErr:  true,
		},
		{
			name: "get connection error: shutdown when waiting for request response",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx := context.Background()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				testReqChan := make(chan connectionRequest)
				pool := newForTesting("", "", 1, 0, testReqChan)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				go func(ch chan connectionRequest, c *ConnectionPool) {
					<-ch
					c.Close()
				}(testReqChan, pool)
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {},
			want:     nil,
			wantErr:  true,
		},
		{
			name: "get connection error: user context canceled when waiting for request response",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx, cancel := context.WithCancel(context.Background())
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				testReqChan := make(chan connectionRequest)
				pool := newForTesting("", "", 1, 0, testReqChan)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				go func(ch chan connectionRequest, cancel context.CancelFunc) {
					<-ch
					cancel()
				}(testReqChan, cancel)
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {
				require.NoError(t, c.Close())
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		t.Run(tt.name, func(t *testing.T) {
			c, ctx, auth, database, query := tt.setup(t, ctrl)
			got, err := c.Query(ctx, auth, database, query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectionPool.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConnectionPool.Query() = %v, want %v", got, tt.want)
			}
			// hack to wait for connection manager handle the returned connection
			time.Sleep(time.Millisecond)
			tt.validate(t, c)
		})
	}
}

func TestConnectionPool_Exec(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*testing.T, *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string)
		validate func(*testing.T, *ConnectionPool)
		want     sql.Result
		wantErr  bool
	}{
		{
			name: "get connection and exec",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "create table"
				idleConnections := orderedmap.New()
				ctx := context.Background()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().ExecContext(ctx, query).Return(nil, nil).Times(1)
				// validates that connection pool is not empty at end of test
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				pool := newForTesting("", "", 1, 0, nil)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {
				require.NoError(t, c.Close())
			},
			want: nil,
		},
		{
			name: "get connection error: shutdown when sending request",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx := context.Background()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				pool := newForTesting("", "", 1, 0, nil)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				err := pool.Close()
				require.NoError(t, err)
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {},
			want:     nil,
			wantErr:  true,
		},
		{
			name: "get connection error: shutdown when waiting for request response",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx := context.Background()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				testReqChan := make(chan connectionRequest)
				pool := newForTesting("", "", 1, 0, testReqChan)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				go func(ch chan connectionRequest, c *ConnectionPool) {
					<-ch
					c.Close()
				}(testReqChan, pool)
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {},
			want:     nil,
			wantErr:  true,
		},
		{
			name: "get connection error: user context canceled when waiting for request response",
			setup: func(t *testing.T, ctrl *gomock.Controller) (*ConnectionPool, context.Context, Auth, string, string) {
				auth := Auth{}
				database := "database"
				query := "select * from table"
				idleConnections := orderedmap.New()
				ctx, cancel := context.WithCancel(context.Background())
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(connectionConfig{
					auth:     auth,
					database: database,
				}, []*connection{
					{
						Connection: conn,
					},
				})
				testReqChan := make(chan connectionRequest)
				pool := newForTesting("", "", 1, 0, testReqChan)
				pool.manager.idleConnections = idleConnections
				pool.manager.totalConnections = 1
				pool.manager.totalIdleConnections = 1
				go func(ch chan connectionRequest, cancel context.CancelFunc) {
					<-ch
					cancel()
				}(testReqChan, cancel)
				return pool, ctx, auth, database, query
			},
			validate: func(t *testing.T, c *ConnectionPool) {
				require.NoError(t, c.Close())
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		t.Run(tt.name, func(t *testing.T) {
			c, ctx, auth, database, query := tt.setup(t, ctrl)
			got, err := c.Exec(ctx, auth, database, query)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectionPool.Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConnectionPool.Exec() = %v, want %v", got, tt.want)
			}
			// hack to wait for connection manager handle the returned connection
			time.Sleep(time.Millisecond)
			tt.validate(t, c)
		})
	}
}
