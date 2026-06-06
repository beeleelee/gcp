.PHONY: gcp

gcp:
	go build -ldflags="-s -w" -o ./bin/gcp ./cmd/gcp