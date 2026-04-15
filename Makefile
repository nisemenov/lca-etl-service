run-service:
	go run cmd/app/main.go

lint:
	gofmt -w .
	goimports -w .

test:
	go test ./...
