.PHONY: gcp callgraph

gcp:
	go build -ldflags="-s -w" -o ./bin/gcp ./cmd/asyncio

callgraph:
	go run ./cmd/callgraph -output gcp_call_chain.svg