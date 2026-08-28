# syntax=docker/dockerfile:1

# Stage 1: Build binaries
FROM golang:alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build all three process binaries statically
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/forgeflow-api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/forgeflow-worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/forgeflow-scheduler ./cmd/scheduler

# Stage 2: Final minimal runtime base
FROM alpine:3.21 AS base

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S appgroup && adduser -S appuser -G appgroup

USER appuser
WORKDIR /app

# Target for API
FROM base AS api
COPY --from=builder /bin/forgeflow-api /app/forgeflow-api
COPY --from=builder /app/web /app/web
EXPOSE 8080 9090
ENTRYPOINT ["/app/forgeflow-api"]

# Target for Worker
FROM base AS worker
COPY --from=builder /bin/forgeflow-worker /app/forgeflow-worker
ENTRYPOINT ["/app/forgeflow-worker"]

# Target for Scheduler
FROM base AS scheduler
COPY --from=builder /bin/forgeflow-scheduler /app/forgeflow-scheduler
ENTRYPOINT ["/app/forgeflow-scheduler"]
