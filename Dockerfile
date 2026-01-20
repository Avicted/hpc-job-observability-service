# Build stage
FROM golang:1.25.5-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Generate OpenAPI internal/api/server and internal/api/types code
RUN go generate ./internal/api/...

# Build the server binary
RUN CGO_ENABLED=1 GOOS=linux go build -o /server ./cmd/server

# Build the mock server binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /mockserver ./cmd/mockserver

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Copy binaries from builder
COPY --from=builder /server /app/server
COPY --from=builder /mockserver /app/mockserver

# Copy OpenAPI spec for mock server
COPY config/openapi.yaml /app/openapi.yaml

# Create non-root user
RUN adduser -D -g '' appuser
USER appuser

# Expose ports
EXPOSE 8080 8081

# Default command runs the main server
CMD ["/app/server"]
