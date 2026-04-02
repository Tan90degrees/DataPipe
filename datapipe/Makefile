.PHONY: build build-master build-worker build-cli test run-master run-worker docker-build clean lint vet

BINARY_DIR := bin
MASTER_BINARY := $(BINARY_DIR)/master
WORKER_BINARY := $(BINARY_DIR)/worker
CLI_BINARY := $(BINARY_DIR)/cli
DOCKER_IMAGE := datapipe/datapipe:latest

build: build-master build-worker build-cli

build-master:
	@mkdir -p $(BINARY_DIR)
	go build -o $(MASTER_BINARY) ./cmd/master

build-worker:
	@mkdir -p $(BINARY_DIR)
	go build -o $(WORKER_BINARY) ./cmd/worker

build-cli:
	@mkdir -p $(BINARY_DIR)
	go build -o $(CLI_BINARY) ./cmd/cli

test:
	go test -v ./...

run-master: build-master
	$(MASTER_BINARY)

run-worker: build-worker
	$(WORKER_BINARY)

docker-build:
	docker build -t $(DOCKER_IMAGE) -f deploy/docker/Dockerfile .

clean:
	rm -rf $(BINARY_DIR)
	rm -f $(MASTER_BINARY) $(WORKER_BINARY) $(CLI_BINARY)

lint:
	golangci-lint run ./...

vet:
	go vet ./...
