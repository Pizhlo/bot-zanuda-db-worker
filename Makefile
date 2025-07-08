mocks:
	go generate ./...

swag:
	swag init --md ./docs --parseInternal  --parseDependency --parseDepth 2 

lint:
	@echo "linting..."
	go vet ./...
	staticcheck ./...
	@echo "linting successfully finished"

test:
	@echo "testing..."
	go test -gcflags="-l" -race -v ./...
	@echo "successfully finished"

all:	
	make lint
	make test

run:
	go run main.go

.PHONY: mocks swag lint test all run