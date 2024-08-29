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
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Middleware to add headers for each request
type ServerMiddlewareAddHeader struct{}

func (s *ServerMiddlewareAddHeader) StartCall(ctx context.Context) context.Context {
	grpc.SetHeader(ctx, metadata.Pairs("foo", "bar"))
	return ctx
}

func (s *ServerMiddlewareAddHeader) CallCompleted(ctx context.Context, err error) {
	grpc.SetTrailer(ctx, metadata.Pairs("super", "duper"))
	if err != nil {
		panic("error detected during server call")
	}
}

// StartServer initializes and starts the Flight SQL server with middleware
func StartServer(address string, srv flight.FlightServer) error {
	// Create middleware for the server
	serverMiddleware := []flight.ServerMiddleware{
		flight.CreateServerMiddleware(&ServerMiddlewareAddHeader{}),
	}

	// Initialize the server with middleware
	server := flight.NewServerWithMiddleware(serverMiddleware)
	server.Init(address)
	server.RegisterFlightService(srv)

	// Graceful shutdown handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Printf("Starting Flight SQL Server on %s...\n", address)
		if err := server.Serve(); err != nil {
			fmt.Printf("Server stopped with error: %v\n", err)
		}
	}()

	<-stop
	fmt.Println("Shutting down Flight SQL Server gracefully...")
	server.Shutdown()
	return nil
}
