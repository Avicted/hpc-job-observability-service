# Build stage
FROM golang:1.25.5-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Generate OpenAPI internal/api/server and internal/api/types code
RUN go generate ./internal/api/...

# Build the server binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /server ./cmd/server

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Copy binaries from builder
COPY --from=builder /server /app/server

# Create non-root user
RUN adduser -D -g '' appuser
USER appuser

# Expose ports
EXPOSE 8080

# Default command runs the main server
CMD ["/app/server"]
