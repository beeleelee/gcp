.PHONY: gcp proto blockio callgraph

gcp:
	go build -ldflags="-s -w" -o ./bin/gcp ./cmd/asyncio

callgraph:
	go run ./cmd/callgraph -output gcp_call_chain.svg

proto:
	protoc --go_out=. --go_opt=paths=source_relative     --go-grpc_out=. --go-grpc_opt=paths=source_relative     blockio/gcp.proto

# DEPRECATED — blockio is archived. Use 'gcp' (asyncio) instead.
blockio:
	@echo "WARNING: blockio is deprecated. Use 'gcp' (asyncio) instead."
	go build -o ./bin/blockio ./cmd/blockio

# blockio_osx:
#		CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o ./bin/blockio_osx ./cmd/blockio

# asyncio:
# 	go build -o ./bin/asyncio ./cmd/asyncio