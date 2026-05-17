.PHONY: gcp proto blockio

gcp:
	go build -ldflags="-s -w" -o ./bin/gcp ./cmd/asyncio

proto:
	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     blockio/gcp.proto

blockio:
	go build -o ./bin/blockio ./cmd/blockio

# blockio_osx:
#		CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./bin/blockio_osx ./cmd/blockio

# asyncio:
# 	go build -o ./bin/asyncio ./cmd/asyncio