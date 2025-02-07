// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	sqlite "github.com/arrowarc/arrowarc/integrations/flight/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Context keys for test purposes
type contextKey string

const (
	fooKey   contextKey = "foo"
	traceKey contextKey = "traceKey"
)

// Middleware implementation to test client behavior with headers
type ClientTestSendHeaderMiddleware struct {
	ctx context.Context
	md  metadata.MD
	mx  sync.Mutex
}

func (c *ClientTestSendHeaderMiddleware) StartCall(ctx context.Context) context.Context {
	c.ctx = metadata.AppendToOutgoingContext(ctx, string(fooKey), "bar")
	return context.WithValue(c.ctx, traceKey, "super")
}

func (c *ClientTestSendHeaderMiddleware) CallCompleted(ctx context.Context, err error) {
	val, _ := ctx.Value(traceKey).(string)
	if val != "super" {
		panic("Invalid context in client middleware")
	}
}

func (c *ClientTestSendHeaderMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	val, _ := ctx.Value(traceKey).(string)
	if val != "super" {
		panic("Invalid context in client middleware")
	}
	c.mx.Lock()
	defer c.mx.Unlock()
	c.md = md
}

// TestClientStreamMiddleware tests the behavior of the client middleware
func TestClientStreamMiddleware(t *testing.T) {
	// Create server middleware for testing headers
	serverMiddleware := []flight.ServerMiddleware{
		flight.CreateServerMiddleware(&sqlite.ServerExpectHeaderMiddleware{}),
		flight.CreateServerMiddleware(&sqlite.ServerMiddlewareAddHeader{}),
	}

	// Initialize the Flight server with middleware
	s := flight.NewServerWithMiddleware(serverMiddleware)
	s.Init("localhost:0")

	// Set up the Flight SQL server (e.g., SQLite-based server)
	db, err := sqlite.CreateDB()
	require.NoError(t, err)
	srv, err := sqlite.NewSQLiteFlightSQLServer(db)
	require.NoError(t, err)
	s.RegisterFlightService(flightsql.NewFlightServer(srv))

	// Start the server in a goroutine
	go s.Serve()
	defer s.Shutdown()

	// Create client middleware for testing
	middleware := &ClientTestSendHeaderMiddleware{}
	client, err := flight.NewClientWithMiddleware(s.Addr().String(), nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(middleware),
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Close()

	// Example client request: ListActions
	flightStream, err := client.ListActions(context.Background(), &flight.Empty{})
	require.NoError(t, err)

	// Process the flight stream
	for {
		info, err := flightStream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
		}
		assert.NotNil(t, info)
		fmt.Println(info)
	}

	// Check headers received by the client middleware
	middleware.mx.Lock()
	defer middleware.mx.Unlock()
	assert.Equal(t, []string{"bar"}, middleware.md.Get("foo"))
	assert.Equal(t, []string{"duper"}, middleware.md.Get("super"))
}
