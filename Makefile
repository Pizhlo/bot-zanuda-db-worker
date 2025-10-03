# заглушает вывод make
MAKEFLAGS+=silent

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
	go test -gcflags="-l" -race -v ./...
	@echo "> successfully finished"

all:	
	@make lint
	@make test

run:
	@echo "> running..."
	go run main.go

.PHONY: mocks swag lint test all run init install-linters