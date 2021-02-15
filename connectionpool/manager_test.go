package connectionpool

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aastein/singlestore/orderedmap"
	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestConnectionManager_appendToIdleConnections(t *testing.T) {
	tests := []struct {
		name     string
		conn     *connection
		numIdle  int
		setup    func() *ConnectionManager
		validate func(connectionConfig, *ConnectionManager)
	}{
		{
			name: "add first idle connection for configuration",
			conn: &connection{
				config: connectionConfig{
					auth:     Auth{Username: "username", Password: "password"},
					database: "database",
				},
			},
			setup: func() *ConnectionManager {
				return &ConnectionManager{
					idleConnections:  orderedmap.New(),
					totalConnections: 0,
				}
			},
			validate: func(config connectionConfig, c *ConnectionManager) {
				assert.Equal(t, 1, c.idleConnections.Len())
				v, ok := c.idleConnections.Get(config)
				assert.True(t, ok)
				assert.Len(t, v.([]*connection), 1)
				assert.Equal(t, 1, c.totalIdleConnections)
			},
		},
		{
			name: "add idle connection to existing configuration",
			conn: &connection{
				config: connectionConfig{
					auth:     Auth{Username: "username", Password: "password"},
					database: "database",
				},
			},
			numIdle: 1,
			setup: func() *ConnectionManager {
				idleConnections := orderedmap.New()
				idleConnections.Set(connectionConfig{
					auth:     Auth{Username: "username", Password: "password"},
					database: "database",
				}, []*connection{
					{
						config: connectionConfig{
							auth:     Auth{Username: "username", Password: "password"},
							database: "database",
						},
					},
				})
				return &ConnectionManager{
					idleConnections:      idleConnections,
					totalIdleConnections: 1,
				}
			},
			validate: func(config connectionConfig, c *ConnectionManager) {
				assert.Equal(t, 1, c.idleConnections.Len())
				v, ok := c.idleConnections.Get(config)
				assert.True(t, ok)
				assert.Len(t, v.([]*connection), 2)
				assert.Equal(t, 2, c.totalIdleConnections)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.setup()
			c.appendToIdleConnections(tt.conn)
			tt.validate(tt.conn.config, c)
		})
	}
}

func TestConnectionManager_closeConnection(t *testing.T) {
	tests := []struct {
		name             string
		connection       func(*gomock.Controller) *connection
		wantErr          bool
		expectTotalConns int
	}{
		{
			name: "close connection decrement count",
			connection: func(ctrl *gomock.Controller) *connection {
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				return &connection{
					Connection: conn,
				}
			},
			expectTotalConns: 0,
		},
		{
			name: "close connection error",
			connection: func(ctrl *gomock.Controller) *connection {
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(errors.New("some error")).Times(1)
				return &connection{
					Connection: conn,
				}
			},
			expectTotalConns: 1,
			wantErr:          true,
		},
	}
	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		t.Run(tt.name, func(t *testing.T) {
			c := &ConnectionManager{
				totalConnections: 1,
			}
			if err := c.closeConnection(tt.connection(ctrl)); (err != nil) != tt.wantErr {
				t.Errorf("ConnectionManager.closeConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.EqualValues(t, tt.expectTotalConns, c.totalConnections)
		})
	}
}

func TestConnectionManager_closeIdleConnections(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*gomock.Controller) *ConnectionManager
		wantErr bool
	}{
		{
			name: "close idle connetions",
			setup: func(ctrl *gomock.Controller) *ConnectionManager {
				idleConnections := orderedmap.New()
				connA := NewMocksqlConnection(ctrl)
				connA.EXPECT().Close().Return(nil).Times(1)
				connB := NewMocksqlConnection(ctrl)
				connB.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set("key not important", []*connection{
					{
						Connection: connA,
					}, {
						Connection: connB,
					},
				})
				return &ConnectionManager{
					idleConnections: idleConnections,
				}
			},
		},
		{
			name: "close idle connetions error",
			setup: func(ctrl *gomock.Controller) *ConnectionManager {
				idleConnections := orderedmap.New()
				connA := NewMocksqlConnection(ctrl)
				connA.EXPECT().Close().Return(errors.New("error")).Times(1)
				idleConnections.Set("key not important", []*connection{
					{
						Connection: connA,
					},
				})
				return &ConnectionManager{
					idleConnections: idleConnections,
				}
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		t.Run(tt.name, func(t *testing.T) {
			c := tt.setup(ctrl)
			if err := c.closeIdleConnections(context.Background()); (err != nil) != tt.wantErr {
				t.Errorf("ConnectionManager.closeIdleConnections() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConnectionManager_fulfillPendingRequests(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(ctrl *gomock.Controller) (*ConnectionManager, []chan *connectionResponse)
		verify  func(*testing.T, *ConnectionManager, []chan *connectionResponse)
		wantErr bool
	}{
		{
			name: "fulfill single pending request with idle connection",
			setup: func(_ *gomock.Controller) (*ConnectionManager, []chan *connectionResponse) {
				config := connectionConfig{}
				responseChan := make(chan *connectionResponse, 1)
				ctx := context.Background()
				request := &connectionRequest{
					config:   config,
					connChan: responseChan,
					ctx:      ctx,
				}
				idleConnections := orderedmap.New()
				idleConnections.Set(config, []*connection{
					{config: config},
				})
				return &ConnectionManager{
					idleConnections: idleConnections,
					pendingRequests: []*connectionRequest{
						request,
					},
					totalConnections:     1,
					totalIdleConnections: 1,
				}, []chan *connectionResponse{responseChan}
			},
			verify: func(t *testing.T, c *ConnectionManager, chans []chan *connectionResponse) {
				assert.NotNil(t, <-chans[0])
				assert.Zero(t, c.idleConnections.Len())
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 0, c.totalIdleConnections)
			},
		},
		{
			name: "fulfill single pending request with new connection",
			setup: func(_ *gomock.Controller) (*ConnectionManager, []chan *connectionResponse) {
				responseChan := make(chan *connectionResponse, 1)
				ctx := context.Background()
				request := &connectionRequest{
					config:   connectionConfig{},
					connChan: responseChan,
					ctx:      ctx,
				}
				idleConnections := orderedmap.New()
				return &ConnectionManager{
					idleConnections: idleConnections,
					pendingRequests: []*connectionRequest{
						request,
					},
					totalConnections:     0,
					totalIdleConnections: 0,
				}, []chan *connectionResponse{responseChan}
			},
			verify: func(t *testing.T, c *ConnectionManager, chans []chan *connectionResponse) {
				assert.NotNil(t, <-chans[0])
				assert.Zero(t, c.idleConnections.Len())
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 0, c.totalIdleConnections)
			},
		},
		{
			name: "fulfill single pending request by canceling idle connection",
			setup: func(ctrl *gomock.Controller) (*ConnectionManager, []chan *connectionResponse) {
				responseChan := make(chan *connectionResponse, 1)
				ctx := context.Background()
				config := connectionConfig{}
				request := &connectionRequest{
					config:   connectionConfig{},
					connChan: responseChan,
					ctx:      ctx,
				}
				idleConnections := orderedmap.New()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set(config, []*connection{
					{
						config:     config,
						Connection: conn,
					},
				})
				return &ConnectionManager{
					idleConnections:      idleConnections,
					connectionLimit:      1,
					totalConnections:     1,
					totalIdleConnections: 1,
					pendingRequests: []*connectionRequest{
						request,
					},
				}, []chan *connectionResponse{responseChan}
			},
			verify: func(t *testing.T, c *ConnectionManager, chans []chan *connectionResponse) {
				assert.NotNil(t, <-chans[0])
				assert.Zero(t, c.idleConnections.Len())
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 0, c.totalIdleConnections)
			},
		},
		{
			name: "request context canceled before fulfilling request",
			setup: func(_ *gomock.Controller) (*ConnectionManager, []chan *connectionResponse) {
				config := connectionConfig{}
				responseChan := make(chan *connectionResponse)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				request := &connectionRequest{
					config:   config,
					connChan: responseChan,
					ctx:      ctx,
				}
				idleConnections := orderedmap.New()
				idleConnections.Set(config, []*connection{
					{config: config},
				})
				return &ConnectionManager{
					idleConnections: idleConnections,
					pendingRequests: []*connectionRequest{
						request,
					},
					totalConnections:     1,
					totalIdleConnections: 1,
				}, []chan *connectionResponse{responseChan}
			},
			verify: func(t *testing.T, c *ConnectionManager, chans []chan *connectionResponse) {
				assert.Equal(t, 1, c.idleConnections.Len())
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 1, c.totalIdleConnections)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c, chans := tt.setup(ctrl)
			openFunc := func(driverName, dataSourceName string) (*sql.DB, error) {
				return &sql.DB{}, nil
			}
			if err := c.fulfillPendingRequests(context.Background(), openFunc); (err != nil) != tt.wantErr {
				t.Errorf("ConnectionManager.fulfillPendingRequests() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.verify(t, c, chans)
		})
	}
}

func TestConnectionManager_tryGetConnection(t *testing.T) {
	tests := []struct {
		name              string
		connectionManager *ConnectionManager
		config            connectionConfig
		want              *connection
		wantErr           bool
	}{
		{
			name: "returns nill connection",
			want: nil,
			connectionManager: &ConnectionManager{
				idleConnections:      orderedmap.New(),
				connectionLimit:      1,
				totalConnections:     1,
				totalIdleConnections: 0,
			},
		},
		// other functionality tested in TestConnectionManager_fulfillPendingRequests
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.connectionManager
			got, err := c.tryGetConnection(context.Background(), tt.config, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConnectionManager.tryGetConnection() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConnectionManager.tryGetConnection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnectionManager_pruneExpiredConnections(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(*gomock.Controller) *ConnectionManager
		config connectionConfig
		verify func(*ConnectionManager)
	}{
		{
			name: "prune expired connections",
			setup: func(ctrl *gomock.Controller) *ConnectionManager {
				idleConnections := orderedmap.New()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				idleConnections.Set("key", []*connection{
					{
						idleStartTime: time.Now().Add(-time.Second * 2),
						Connection:    conn,
					},
				})
				return &ConnectionManager{
					idleTimeout:          time.Second,
					idleConnections:      idleConnections,
					totalConnections:     1,
					totalIdleConnections: 1,
				}
			},
			verify: func(c *ConnectionManager) {
				assert.Equal(t, 0, c.idleConnections.Len())
				assert.Equal(t, 0, c.totalConnections)
				assert.Equal(t, 0, c.totalIdleConnections)
			},
		},
		{
			name: "do not prune connection",
			setup: func(ctrl *gomock.Controller) *ConnectionManager {
				idleConnections := orderedmap.New()
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(0)
				idleConnections.Set("key", []*connection{
					{
						idleStartTime: time.Now(),
						Connection:    conn,
					},
				})
				return &ConnectionManager{
					idleTimeout:          time.Hour,
					idleConnections:      idleConnections,
					totalConnections:     1,
					totalIdleConnections: 1,
				}
			},
			verify: func(c *ConnectionManager) {
				assert.Equal(t, 1, c.idleConnections.Len())
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 1, c.totalIdleConnections)
			},
		},
	}
	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		t.Run(tt.name, func(t *testing.T) {
			c := tt.setup(ctrl)
			c.pruneExpiredConnections(&connectionReceive{ctx: context.Background()})
		})
	}
}

func TestConnectionManager_handleConnectionReceive(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(*gomock.Controller) (*ConnectionManager, *connectionReceive)
		validate func(*testing.T, *ConnectionManager)
		wantErr  bool
	}{
		{
			name: "close errored connection",
			setup: func(ctrl *gomock.Controller) (*ConnectionManager, *connectionReceive) {
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				rec := &connectionReceive{
					conn: &connection{
						Connection: conn,
					},
					ctx: context.Background(),
					err: errors.New("error"),
				}
				return &ConnectionManager{
					idleConnections: orderedmap.New(),
				}, rec
			},
			validate: func(t *testing.T, cm *ConnectionManager) {
				assert.Equal(t, cm.idleConnections.Len(), 0)
			},
		},
		{
			name: "append to idle connections",
			setup: func(ctrl *gomock.Controller) (*ConnectionManager, *connectionReceive) {
				conn := NewMocksqlConnection(ctrl)
				conn.EXPECT().Close().Return(nil).Times(1)
				rec := &connectionReceive{
					conn: &connection{
						Connection: conn,
					},
					ctx: context.Background(),
				}
				return &ConnectionManager{
					idleConnections: orderedmap.New(),
				}, rec
			},
			validate: func(t *testing.T, cm *ConnectionManager) {
				assert.Equal(t, cm.idleConnections.Len(), 1)
			},
		},
	}
	for _, tt := range tests {
		ctrl := gomock.NewController(t)
		t.Run(tt.name, func(t *testing.T) {
			c, rec := tt.setup(ctrl)
			fn := func(driverName, dataSourceName string) (*sql.DB, error) {
				return &sql.DB{}, nil
			}
			if err := c.handleConnectionReceive(rec, fn); (err != nil) != tt.wantErr {
				t.Errorf("ConnectionManager.handleConnectionReceive() error = %v, wantErr %v", err, tt.wantErr)
			}
			tt.validate(t, c)
		})
	}
}

func TestConnectionManager_handleConnectionRequest(t *testing.T) {
	tests := []struct {
		name       string
		fnOpenConn sqlOpenFunc
		setup      func() (*ConnectionManager, *connectionRequest)
		validate   func(*testing.T, *ConnectionManager, chan *connectionResponse)
	}{
		{
			name: "respond with error",
			fnOpenConn: func(driverName, dataSourceName string) (*sql.DB, error) {
				return nil, errors.New("error")
			},
			setup: func() (*ConnectionManager, *connectionRequest) {
				return &ConnectionManager{
						idleConnections: orderedmap.New(),
					}, &connectionRequest{
						config:   connectionConfig{},
						connChan: make(chan *connectionResponse, 1),
						ctx:      context.Background(),
					}
			},
			validate: func(t *testing.T, c *ConnectionManager, ch chan *connectionResponse) {
				resp := <-ch
				assert.Error(t, resp.err)
				assert.Nil(t, resp.conn)
				assert.Equal(t, 0, c.totalConnections)
				assert.Equal(t, 0, c.totalIdleConnections)
			},
		},
		{
			name: "respond with connection",
			fnOpenConn: func(driverName, dataSourceName string) (*sql.DB, error) {
				return &sql.DB{}, nil
			},
			setup: func() (*ConnectionManager, *connectionRequest) {
				return &ConnectionManager{
						idleConnections: orderedmap.New(),
					}, &connectionRequest{
						config:   connectionConfig{},
						connChan: make(chan *connectionResponse, 1),
						ctx:      context.Background(),
					}
			},
			validate: func(t *testing.T, c *ConnectionManager, ch chan *connectionResponse) {
				resp := <-ch
				assert.NoError(t, resp.err)
				assert.NotNil(t, resp.conn)
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 0, c.totalIdleConnections)
			},
		},
		{
			name: "append connection to idle connections",
			fnOpenConn: func(driverName, dataSourceName string) (*sql.DB, error) {
				return &sql.DB{}, nil
			},
			setup: func() (*ConnectionManager, *connectionRequest) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return &ConnectionManager{
						idleConnections: orderedmap.New(),
					}, &connectionRequest{
						config: connectionConfig{},
						ctx:    ctx,
					}
			},
			validate: func(t *testing.T, c *ConnectionManager, ch chan *connectionResponse) {
				assert.Equal(t, 1, c.idleConnections.Len())
				assert.Equal(t, 1, c.totalConnections)
				assert.Equal(t, 1, c.totalIdleConnections)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, req := tt.setup()
			c.handleConnectionRequest(req, tt.fnOpenConn)
			tt.validate(t, c, req.connChan)
		})
	}
}
