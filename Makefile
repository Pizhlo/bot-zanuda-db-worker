mocks:
	go generate ./...

swag:
	swag init --md ./docs --parseInternal  --parseDependency --parseDepth 2 

init:
	@make install-linters

install-linters:
	@echo "> installing linters..."
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@echo "> golangci-lint installed successfully"
	golangci-lint --version
	@echo "> linters installed successfully"

lint:
	@echo "> linting..."
	go vet ./...
	staticcheck ./...
	golangci-lint run ./...
	@echo "> linting successfully finished"

test:
	@echo "> testing..."
	go test -gcflags="-l" -race -v ./...
	@echo "> successfully finished"

all:	
	@echo "> linting..."
	make lint
	@echo "> testing..."
	make test

run:
	@echo "> running..."
	go run main.go

.PHONY: mocks swag lint test all run init install-linters