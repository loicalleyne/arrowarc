# Stage 1: Build the Binary
FROM golang:1.23.0-bookworm AS build

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download && go mod verify

COPY . .

# Build the Go application
# - CGO_ENABLED=0: Disable CGO for a statically linked binary
# - GOOS=linux GOARCH=amd64: Target Linux OS with x86_64 architecture
# - -o arrowarc: Output binary named 'arrowarc'
# - -a: Force rebuilding of packages
# - -ldflags="-s -w": Strip debug information to reduce binary size
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -o arrowarc -a -ldflags="-s -w"

# Stage 2: Create a minimal runtime image
FROM scratch

# Copy the binary from the build stage
COPY --from=build /app/arrowarc /arrowarc

# Set metadata labels
LABEL maintainer="mcgeehan@gmail.com" \
      version="0.1.0" \
      publisher="Thomas F McGeehan V" \
      description="ArrowArc is a high-performance data exchange platform."

# Set the entrypoint to the binary
ENTRYPOINT ["/arrowarc"]
