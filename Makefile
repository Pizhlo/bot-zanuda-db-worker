MAKEFILE_PATH := $(abspath $(firstword $(MAKEFILE_LIST)))
CUR_DIR := $(patsubst %/,%, $(dir $(MAKEFILE_PATH)))
BUILD_DIR := $(CUR_DIR)/.build
APP_EXECUTABLE_DIR := $(BUILD_DIR)/bin

# заглушает вывод make
# MAKEFLAGS+=silent # временно отключено, пока не сделана задача BZ-26

mocks:
	@echo "> generating mocks..."
	go generate ./...
	@echo "> mocks generated successfully"

swag:
	@echo "> generating swagger documentation..."
	swag init --md ./docs --parseInternal  --parseDependency --parseDepth 2 
	@echo "> swagger documentation generated successfully"

init:
	@echo "> initializing..."
	@make install-linters

install-linters:
	@echo "> installing linters..."
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@echo "> golangci-lint installed successfully"
	golangci-lint --version
	
	@echo "> installing staticcheck"
	go install honnef.co/go/tools/cmd/staticcheck@latest
	@echo "> staticcheck installed successfully"
	staticcheck --version
	
	@echo "> linters installed successfully"

lint:
	@echo "> linting..."
	go vet ./...
	staticcheck ./...
	golangci-lint run ./...
	@echo "> linting successfully finished"

test:
	@echo "> testing..."
	go test -cover -gcflags="-l" -race -v ./...
	@echo "> successfully finished"

all:	
	@make lint
	@make test
	@make build

build:
	@echo " > building..."
	@mkdir -p "$(BUILD_DIR)/bin"
	@VERSION=$$(git describe --tags --always --dirty); \
	BUILD_DATE=$$(date -u +%Y%m%d-%H%M%SZ); \
	GIT_COMMIT=$$(git rev-parse --short HEAD); \
	go build -trimpath \
	-ldflags "-s -w -X main.Version=$$VERSION -X main.BuildDate=$$BUILD_DATE -X main.GitCommit=$$GIT_COMMIT" \
	-o "$(BUILD_DIR)/bin/" ./cmd/...
	@echo " > successfully built"


run:
	@make build
	$(APP_EXECUTABLE_DIR)/app

.PHONY: mocks swag lint test all run init install-linters