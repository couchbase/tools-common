PACKAGE=
TESTS=

coverage:
	@go test ./$(PACKAGE)/... -run=$(TESTS) -count=1 -covermode=atomic -coverprofile=coverage.out -failfast -shuffle=on && go tool cover -html=coverage.out

lint:
	@golangci-lint run

test:
	@go test ./$(PACKAGE)/... -run=$(TESTS) -count=1 -cover -failfast -shuffle=on -parallel 1

clean:
	@rm -f coverage.out

.PHONY: coverage test lint clean
