# gcp — agent instructions

Copy files from local to a remote host over TCP. Two implementations in one repo.

> **Note:** The `blockio` (gRPC) module is **deprecated**. It is kept for reference but will not receive new features. Use the `gcp` (asyncio) module instead.

## Binaries

| make target | source dir              | output binary    | transport          |
|-------------|-------------------------|------------------|--------------------|
| `gcp`       | `cmd/asyncio/`          | `./bin/gcp`      | custom TCP + CBOR  |
| ~~`blockio`~~ | ~~`cmd/blockio/`~~          | ~~`./bin/blockio`~~  | ~~gRPC~~ (deprecated) |

Both default to port `1717`.

## Build commands

```
make gcp        # builds asyncio binary with -ldflags="-s -w"
make blockio    # builds gRPC binary (deprecated)
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
./bin/gcp cp --chunk 32768 --batch 4 ./src.txt host:1717/remote/path

# download from host
./bin/gcp cp --chunk 32768 --batch 4 host:1717/remote/path ./local_dst

# with reliability options
./bin/gcp cp --timeout 15s --retry 3 --checksum true ./src.txt host:1717/dst

# copy directory recursively
./bin/gcp cp -r ./mydir host:1717/dst
```

Blockio (gRPC variant, deprecated):
```
# serve
./bin/blockio serve --listen :1717

# copy
./bin/blockio cp --host localhost:1717 --chunk 32768 --batch 16 ./src.txt /remote/path

# download from host
./bin/blockio cp --from /remote/path --host localhost:1717 --chunk 32768 --batch 16 ./local_dst
```

Log level: `--level` or `-L` flag (`error` default, accepts `info`, `debug`, etc.).

### `cp` flags

| Flag          | Default | Description                            |
|---------------|---------|----------------------------------------|
| `--chunk`     | 32768   | chunk size in bytes                    |
| `--batch`     | 4       | max concurrent chunk transfers         |
| `--timeout`   | 15s     | read/write timeout (0 to disable)      |
| `--retry`     | 3       | max chunk transfer retries             |
| `--checksum`  | true    | enable CRC-32 chunk integrity checks   |
| `--recursive, -r` | false | copy directories recursively           |

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
- Both variants support `--from` for download: `./bin/gcp cp --from /remote/path ./local_dst`

## Known bugs resolved

- **2026-05-22**: Client hang on upload (`handleReceive` infinite loop). Root cause: `client.go:handleReceive` checked `readSize == asyncio.HeadSize` to parse headers and allocate buffers, but `readSize` was never incremented in that branch. This caused a busy-loop that allocated memory on every iteration without ever reading response data. Fix: changed condition to `len(bufMsg) == 0`, which only fires once after the header is read.


## Prebuilt binaries

Prebuilt binaries live in `./bin/` (gitignored): `asyncio`, `blockio`, `blockio_osx`.
