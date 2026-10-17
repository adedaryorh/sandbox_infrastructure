.PHONY: build run test docker-build docker-push k8s-apply clean

# ── Build ─────────────────────────────────────────────────────────────────────

# Build the server binary
build:
	go build -o bin/server ./cmd/server

# Build the CLI binary
build-cli:
	go build -o bin/sandbox ./cli/cmd

# Build both
build-all: build build-cli

# Run locally (needs KUBECONFIG and DATABASE_URL set)
run:
	go run ./cmd/server

# Run with hot reload (install: go install github.com/cosmtrek/air@latest)
dev:
	air -c .air.toml

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	go test ./... -v -timeout 30s

test-race:
	go test ./... -race -timeout 60s

# ── Docker ────────────────────────────────────────────────────────────────────

REGISTRY ?= yourorg
IMAGE     ?= sandboxing
TAG       ?= latest

docker-build:
	docker build -t $(REGISTRY)/$(IMAGE):$(TAG) .

docker-push:
	docker push $(REGISTRY)/$(IMAGE):$(TAG)

docker-build-push: docker-build docker-push

# ── Local dev stack ───────────────────────────────────────────────────────────

# Start postgres + caddy locally
up:
	docker-compose up -d postgres caddy

# Start full stack including the server
up-all:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f server

# ── Kubernetes ────────────────────────────────────────────────────────────────

# Apply all manifests in order
k8s-apply:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/runtimeclass.yaml
	kubectl apply -f k8s/storage.yaml
	kubectl apply -f k8s/postgres.yaml
	kubectl apply -f k8s/orchestrator.yaml
	kubectl apply -f k8s/ingress.yaml

# Roll out a new image version
k8s-deploy:
	kubectl set image deployment/orchestrator \
		orchestrator=$(REGISTRY)/$(IMAGE):$(TAG) \
		-n sandboxes

# Watch pod status
k8s-status:
	kubectl get pods -n sandboxes -w

# Tail orchestrator logs
k8s-logs:
	kubectl logs -n sandboxes -l app=orchestrator -f --tail=100

# ── Database ──────────────────────────────────────────────────────────────────

# Connect to local postgres
psql:
	psql "postgres://sandboxing:sandboxing@localhost:5432/sandboxing"

# ── Misc ──────────────────────────────────────────────────────────────────────

# Generate client SDKs using openapi-generator
generate-sdk:
	docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
		-i /local/internal/api/openapi.yaml \
		-g python \
		-o /local/sdk/python
	docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
		-i /local/internal/api/openapi.yaml \
		-g typescript-axios \
		-o /local/sdk/typescript
	docker run --rm -v "${PWD}:/local" openapitools/openapi-generator-cli generate \
		-i /local/internal/api/openapi.yaml \
		-g go \
		-o /local/sdk/go

tidy:
	go mod tidy

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/
