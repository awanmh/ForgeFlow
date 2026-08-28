.PHONY: all build test test-race vet lint docker-build docker-up docker-down clean help

SHELL := /bin/bash
BIN_DIR := bin

all: build test-race vet

help: ## Display available make targets
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all service binaries locally
	@mkdir -p $(BIN_DIR)
	@echo "Building forgeflow-api..."
	go build -o $(BIN_DIR)/forgeflow-api ./cmd/api
	@echo "Building forgeflow-worker..."
	go build -o $(BIN_DIR)/forgeflow-worker ./cmd/worker
	@echo "Building forgeflow-scheduler..."
	go build -o $(BIN_DIR)/forgeflow-scheduler ./cmd/scheduler
	@echo "Build completed successfully."

test: ## Run unit tests
	go test -v ./...

test-race: ## Run all tests with Go race detector
	go test -v -race ./...

vet: ## Run Go static analysis
	go vet ./...

lint: ## Run golangci-lint if installed
	@which golangci-lint > /dev/null && golangci-lint run ./... || echo "golangci-lint not installed, skipping"

docker-build: ## Build docker images
	docker compose build

docker-up: ## Start all infrastructure and services via Docker Compose
	docker compose up --build -d

docker-down: ## Stop all services and clean network
	docker compose down

clean: ## Remove compiled binaries
	rm -rf $(BIN_DIR)
