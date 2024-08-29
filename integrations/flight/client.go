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

package integrations

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Define a specific type for context keys
type contextKey string

const headerContextKey contextKey = "header"

// ClientMiddleware adds headers and handles received metadata
type ClientMiddleware struct {
	ctx context.Context
	md  metadata.MD
	mx  sync.Mutex
}

func (c *ClientMiddleware) StartCall(ctx context.Context) context.Context {
	c.ctx = context.WithValue(metadata.AppendToOutgoingContext(ctx, "foo", "bar"), headerContextKey, "super")
	return c.ctx
}

func (c *ClientMiddleware) CallCompleted(ctx context.Context, err error) {
	val, ok := ctx.Value(headerContextKey).(string)
	if !ok || val != "super" {
		panic("invalid context in client middleware")
	}
}

func (c *ClientMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.md = md
}

// NewFlightClient initializes a new Flight client with middleware
func NewFlightClient(address string) (flight.FlightServiceClient, error) {
	cookieMiddleware := flight.NewCookieMiddleware() // Required cookie middleware

	client, err := flight.NewClientWithMiddleware(address, nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(cookieMiddleware),
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create Flight client: %w", err)
	}
	return client, nil
}
