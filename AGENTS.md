# gcp — agent instructions

Copy files between local and hosts.

## Binaries

| make target | source dir     | output binary | transport         |
|-------------|----------------|---------------|-------------------|
| `gcp`       | `cmd/asyncio/` | `./bin/gcp`   | custom TCP + CBOR |

The asyncio variant defaults to port `5031`.

## Build commands

```
make gcp        # builds asyncio binary with -ldflags="-s -w"
```

No CI or linter config. Tests live in `message/`, `logger/`, `cmd/progressbar/`.
```
go test -count=1 ./...        # all tests
go test -race -count=1 ./...  # with race detector
```

## CLI usage

Asyncio (default):
```
# serve on host
./bin/gcp serve --listen tcp://0.0.0.0:5031 --process-num 4 --multicore true

# copy file from local to host
./bin/gcp cp --chunk 32768 --batch 4 ./src.txt host:5031/remote/path

# download from host
./bin/gcp cp --chunk 32768 --batch 4 host:5031/remote/path ./local_dst

# with reliability options and compression
./bin/gcp cp --timeout 15s --retry 3 --checksum true --compression gzip ./src.txt host:5031/dst

# copy directory recursively
./bin/gcp cp -r ./mydir host:5031/dst

# dry-run (show what would be transferred without doing it)
./bin/gcp cp --dry-run ./src.txt host:5031/dst
```

Log level: `--level` or `-L` flag (`error` default, accepts `info`, `debug`, etc.).

### `cp` flags

| Flag               | Default | Description                                 |
|--------------------|---------|---------------------------------------------|
| `--chunk`          | 32768   | chunk size in bytes                         |
| `--batch`          | 4       | max concurrent chunk transfers              |
| `--timeout`        | 15s     | read/write timeout (0 to disable)           |
| `--retry`          | 3       | max chunk transfer retries                  |
| `--checksum`       | true    | enable CRC-32 chunk integrity checks        |
| `--recursive, -r`  | false   | copy directories recursively                |
| `--dry-run`        | false   | show transfer plan without doing it         |
| `--compression`    | ""      | gzip chunk payloads (`gzip` or empty)       |

## Architecture notes

- `cmd/asyncio/` is the primary/main CLI entrypoint. The module root is **not** a `main` package.
- Asyncio uses `gnet` (event-loop networking) + CBOR message encoding with a custom binary protocol (magic bytes `0xA8 0xD5`).
- `message/` package is the shared protocol library (message types, encode/decode). Not specific to either variant.
- `logger/` wraps `zerolog` into a `slog.Logger` via `zerolog.NewSlogHandler`.
- `cmd/progressbar/` provides a real-time progress display using `go-humanize`.

## Important quirks

- `make gcp` strips debug info (`-ldflags="-s -w"`); use `go build` directly from `cmd/asyncio` for debugging.
- Asyncio server never closes `CreateRes`/`WriteRes` connections on client response — it keeps the conn open.
- The asyncio variant supports `--from` for download: `./bin/gcp cp --from /remote/path ./local_dst`

## Known bugs resolved

- **2026-05-22**: Client hang on upload (`handleReceive` infinite loop). Root cause: `client.go:handleReceive` checked `readSize == message.HeadSize` to parse headers and allocate buffers, but `readSize` was never incremented in that branch. This caused a busy-loop that allocated memory on every iteration without ever reading response data. Fix: changed condition to `len(bufMsg) == 0`, which only fires once after the header is read.


## Prebuilt binaries

Prebuilt binaries live in `./bin/` (gitignored): `asyncio`.
