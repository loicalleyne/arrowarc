// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.18
// +build go1.18

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/flight/flightsql"
	sqllite "github.com/arrowarc/arrowarc/integrations/flightsql/sqllite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	var (
		host = flag.String("host", "localhost", "hostname to bind to")
		port = flag.Int("port", 12345, "port to bind to") // Set a default port
	)

	flag.Parse()

	// Create the in-memory SQLite database
	db, err := sqllite.CreateDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create the SQLiteFlightSQL server
	srv, err := sqllite.NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new gRPC server
	grpcServer := grpc.NewServer()

	// Create a new FlightSQL service instance
	flightServer := flightsql.NewFlightServer(srv)

	// Register the Flight SQL service with the gRPC server
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	// Enable gRPC reflection
	reflection.Register(grpcServer)

	// Start listening on the specified address
	listener, err := net.Listen("tcp", net.JoinHostPort(*host, strconv.Itoa(*port)))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Gracefully handle shutdown on interrupt signals
	fmt.Printf("Starting SQLite Flight SQL Server on %s...\n", listener.Addr().String())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
