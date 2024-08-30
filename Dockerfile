# Stage 1: Build arrow-adbc with DuckDB and PostgreSQL integration
FROM debian:bullseye AS adbc-builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    git \
    cmake \
    build-essential \
    libssl-dev \
    python3 \
    python3-pip \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install meson and ninja
RUN pip3 install meson ninja

# Clone arrow-adbc repository
RUN git clone https://github.com/apache/arrow-adbc.git

# Build arrow-adbc with DuckDB and PostgreSQL integration
WORKDIR /arrow-adbc
RUN mkdir build && cd build && \
    cmake .. -DADBC_DUCKDB_INTEGRATION=ON -DADBC_DRIVER_POSTGRESQL=ON -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc)

# Stage 2: Build the Go application
FROM golang:1.23.0-bullseye AS build

WORKDIR /app

# Copy arrow-adbc libraries from the previous stage
COPY --from=adbc-builder /arrow-adbc/build/adbc_driver_duckdb.so /usr/local/lib/
COPY --from=adbc-builder /arrow-adbc/build/adbc_driver_postgresql.so /usr/local/lib/
COPY --from=adbc-builder /arrow-adbc/build/libadbc_driver_manager.so /usr/local/lib/

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies
RUN go mod download

# Copy the source from the current directory to the working Directory inside the container
COPY . .

# Build the Go app
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -o arrowarc -a -ldflags="-s -w" ./cmd/arrowarc

# Stage 3: Create the final image
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy necessary libraries and the binary
COPY --from=adbc-builder /arrow-adbc/build/adbc_driver_duckdb.so /usr/local/lib/
COPY --from=adbc-builder /arrow-adbc/build/adbc_driver_postgresql.so /usr/local/lib/
COPY --from=adbc-builder /arrow-adbc/build/libadbc_driver_manager.so /usr/local/lib/
COPY --from=build /app/arrowarc /arrowarc

# Set library path
ENV LD_LIBRARY_PATH=/usr/local/lib

# Set the entrypoint
ENTRYPOINT ["/arrowarc"]