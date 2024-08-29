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
	"database/sql"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	sqllite "github.com/arrowarc/arrowarc/integrations/flight/flightsql/sqlite"
	"github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

type SQLiteFlightSQLSuite struct {
	suite.Suite
	server     *sqllite.SQLiteFlightSQLServer
	flightSrv  *flight.Server
	grpcServer *grpc.Server
	db         *sql.DB
	listener   *bufconn.Listener
}

func (suite *SQLiteFlightSQLSuite) SetupSuite() {
	var err error

	// Create the in-memory SQLite database
	suite.db, err = sqllite.CreateDB()
	suite.Require().NoError(err, "failed to create SQLite database")

	// Create the SQLiteFlightSQL server
	suite.server, err = sqllite.NewSQLiteFlightSQLServer(suite.db)
	suite.Require().NoError(err, "failed to create SQLite Flight SQL Server")

	// Create the gRPC server
	suite.grpcServer = grpc.NewServer()
	baseFlightServer := flightsql.NewFlightServer(suite.server)

	// Register the Flight SQL service with the gRPC server
	flight.RegisterFlightServiceServer(suite.grpcServer, baseFlightServer)

	// Create an in-memory listener
	suite.listener = bufconn.Listen(bufSize)

	// Create a channel to capture any errors from the goroutine
	errChan := make(chan error, 1)

	// Start the Flight SQL server in a goroutine and capture any errors
	go func() {
		errChan <- suite.grpcServer.Serve(suite.listener)
		close(errChan)
	}()

	// Wait for a short period to ensure the server starts up properly
	time.Sleep(100 * time.Millisecond)

	// Check if there were any errors starting the server
	select {
	case err := <-errChan:
		if err != nil && err != grpc.ErrServerStopped {
			suite.T().Fatal(err) // Fail the test if there was an error
		}
	default:
		// No errors, continue
	}
}

func (suite *SQLiteFlightSQLSuite) TearDownSuite() {
	// Shutdown the gRPC server gracefully
	suite.grpcServer.GracefulStop()

	// Close the in-memory listener and SQLite database
	suite.listener.Close()
	suite.db.Close()
}

// Dialer provides a dialer for the bufconn listener.
func (suite *SQLiteFlightSQLSuite) Dialer(ctx context.Context, target string) (net.Conn, error) {
	return suite.listener.Dial()
}

func (suite *SQLiteFlightSQLSuite) TestFlightSQLQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suite.T().Log("Attempting to create a gRPC client connection...")
	conn, err := grpc.NewClient("bufnet", grpc.WithContextDialer(suite.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	suite.Require().NoError(err, "failed to create gRPC client")
	defer conn.Close()

	client := flight.NewFlightServiceClient(conn)
	stream, err := client.Handshake(ctx)
	suite.Require().NoError(err, "failed to initiate Handshake with server")

	suite.T().Log("Sending handshake request...")
	err = stream.Send(&flight.HandshakeRequest{Payload: []byte("test")})
	suite.Require().NoError(err, "failed to send handshake request")

	resp, err := stream.Recv()
	if err == io.EOF {
		suite.T().Fatal("Server closed connection unexpectedly")
	} else {
		suite.Require().NoError(err, "failed to receive handshake response")
	}
	suite.NotNil(resp, "expected a response from handshake")

	suite.T().Logf("Received payload: %s", string(resp.Payload))
}

// Run the test suite
func TestSQLiteFlightSQLSuite(t *testing.T) {
	utils.LoadEnv()
	// Skip this test in CI environment if GCP credentials are not set
	if os.Getenv("CI") == "true" || os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("Skipping Flight SQL integration test in CI environment until stabalized.")
	}
	suite.Run(t, new(SQLiteFlightSQLSuite))
}
