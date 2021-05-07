coverage:
	@go test -coverprofile=coverage.out ./... && go tool cover -html=coverage.out

lint:
	@golangci-lint run

test:
	@go test -count=1 -cover ./...

clean:
	@rm -f coverage.out

.PHONY: coverage test lint clean
