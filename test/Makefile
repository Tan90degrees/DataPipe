.PHONY: test-unit test-integration test-all test-coverage test-unit-verbose test-integration-verbose

UNIT_DIR := unit
INTEGRATION_DIR := integration
COVERAGE_DIR := coverage
COVERAGE_FILE := $(COVERAGE_DIR)/coverage.out

test-unit:
	cd $(UNIT_DIR) && go test -v ./...

test-unit-verbose:
	cd $(UNIT_DIR) && go test -v -race ./...

test-integration:
	cd $(INTEGRATION_DIR) && go test -v ./...

test-integration-verbose:
	cd $(INTEGRATION_DIR) && go test -v -race ./...

test-all: test-unit test-integration

test-coverage:
	mkdir -p $(COVERAGE_DIR)
	cd $(UNIT_DIR) && go test -v -coverprofile=$(COVERAGE_DIR)/unit.out ./...
	cd $(INTEGRATION_DIR) && go test -v -coverprofile=$(COVERAGE_DIR)/integration.out ./...
	cd $(UNIT_DIR) && go tool cover -html=$(COVERAGE_DIR)/unit.out -o $(COVERAGE_DIR)/unit.html
	cd $(INTEGRATION_DIR) && go tool cover -html=$(COVERAGE_DIR)/integration.out -o $(COVERAGE_DIR)/integration.html
	@echo "Coverage reports generated:"
	@echo "  - $(COVERAGE_DIR)/unit.html"
	@echo "  - $(COVERAGE_DIR)/integration.html"

test-coverage-combined:
	mkdir -p $(COVERAGE_DIR)
	cd $(UNIT_DIR) && go test -v -coverprofile=$(COVERAGE_DIR)/unit.out ./...
	cd $(INTEGRATION_DIR) && go test -v -coverprofile=$(COVERAGE_DIR)/integration.out ./...
	cat $(COVERAGE_DIR)/unit.out $(COVERAGE_DIR)/integration.out > $(COVERAGE_DIR)/combined.out
	go tool cover -html=$(COVERAGE_DIR)/combined.out -o $(COVERAGE_DIR)/combined.html
	@echo "Combined coverage report: $(COVERAGE_DIR)/combined.html"

clean-coverage:
	rm -rf $(COVERAGE_DIR)

help:
	@echo "Available targets:"
	@echo "  test-unit              - Run unit tests"
	@echo "  test-unit-verbose      - Run unit tests with race detector"
	@echo "  test-integration       - Run integration tests"
	@echo "  test-integration-verbose - Run integration tests with race detector"
	@echo "  test-all               - Run all tests (unit + integration)"
	@echo "  test-coverage          - Generate coverage reports for each test suite"
	@echo "  test-coverage-combined - Generate combined coverage report"
	@echo "  clean-coverage         - Remove coverage reports"
