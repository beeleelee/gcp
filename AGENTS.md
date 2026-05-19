# gcp — agent instructions

Copy files from local to a remote host over TCP. Two implementations in one repo.

## Binaries

| make target | source dir              | output binary    | transport          |
|-------------|-------------------------|------------------|--------------------|
| `gcp`       | `cmd/asyncio/`          | `./bin/gcp`      | custom TCP + CBOR  |
| `blockio`   | `cmd/blockio/`          | `./bin/blockio`  | gRPC               |

Both default to port `1717`.

## Build commands

```
make gcp        # builds asyncio binary with -ldflags="-s -w"
make blockio    # builds gRPC binary
make proto      # regenerate blockio/gcp.pb.go and gcp_grpc.pb.go from blockio/gcp.proto
```

No CI or linter config. Tests live in `asyncio/`, `logger/`, `cmd/progressbar/`.
```
go test -count=1 ./...        # all tests
go test -race -count=1 ./...  # with race detector
```

## CLI usage

Asyncio (default):
```
# serve on host
./bin/gcp serve --listen tcp://0.0.0.0:1717 --process-num 4 --multicore true

# copy file from local to host
./bin/gcp cp --host localhost:1717 --chunk 32768 --batch 4 ./src.txt /remote/path
```

Blockio (gRPC variant):
```
# serve
./bin/blockio serve --listen :1717

# copy
./bin/blockio cp --host localhost:1717 --chunk 32768 --batch 16 ./src.txt /remote/path
```

Log level: `--level` or `-L` flag (`error` default, accepts `info`, `debug`, etc.).

## Architecture notes

- `cmd/asyncio/` is the primary/main CLI entrypoint. The module root is **not** a `main` package.
- Asyncio uses `gnet` (event-loop networking) + CBOR message encoding with a custom binary protocol (magic bytes `0xA8 0xD5`).
- Blockio uses standard gRPC (protobuf). Proto definition in `blockio/gcp.proto`; generated code is committed.
- `asyncio/` package is the shared protocol library (message types, encode/decode). Not specific to either variant.
- `logger/` wraps `zerolog` into a `slog.Logger` via `zerolog.NewSlogHandler`.
- `cmd/progressbar/` provides a real-time progress display using `go-humanize`.

## Important quirks

- `make gcp` strips debug info (`-ldflags="-s -w"`); use `go build` directly from `cmd/asyncio` for debugging.
- gRPC `WriteReq.Data` holds the file chunk bytes — large payloads are expected.
- Asyncio server never closes `CreateRes`/`WriteRes` connections on client response — it keeps the conn open.
- `blockio/cp.go` does not call `cc.Create()` with `Size` field (unlike `asyncio/cp.go` which passes `sfinfo.Size()`). The `blockio` proto has a `size` field in `CreateReq` but the cp caller omits it — likely a bug or benign because the file gets written by offset.

## Prebuilt binaries

Prebuilt binaries live in `./bin/` (gitignored): `asyncio`, `blockio`, `blockio_osx`.
