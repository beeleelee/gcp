# gcp

Copy files between local and hosts over TCP.

## Build

```
make gcp        # builds asyncio binary: ./bin/gcp
```

Tests:
```
go test -count=1 ./...
go test -race -count=1 ./...
```

## Usage

### Serve (on remote host)

```
./bin/gcp serve --listen tcp://0.0.0.0:5031 --process-num 4 --multicore true
```

### Upload (local to remote)

```
./bin/gcp cp ./src.txt host:5031/remote/path
./bin/gcp cp -r ./mydir host:5031/remote/dir
```

Omitting the remote path or ending with `/` appends the source basename:

```
./bin/gcp cp ./src.txt host:5031/
# copies to <remote>/src.txt
```

### Download (remote to local)

```
./bin/gcp cp host:5031/remote/path ./local_dst
./bin/gcp cp -r host:5031/remote/dir ./local_dir
```

Omitting the local path downloads to the current directory:

```
./bin/gcp cp host:5031/remote/src.txt
# downloads to ./src.txt
```

### Multi-source and wildcards

```
# multiple files
./bin/gcp cp a.txt b.txt c.txt host:5031/dst/

# local glob (quoted to prevent shell expansion)
./bin/gcp cp 'src/*.txt' host:5031/dst/

# recursive local glob (walks directories matched by the pattern)
./bin/gcp cp -r '/src/dir/*' host:5031/dst/

# remote glob (matched against ReadDir entries)
./bin/gcp cp 'host:5031/dir/*.txt' ./local/
```

When copying multiple sources, the destination is treated as a directory.

### Dry-run

```
./bin/gcp cp --dry-run ./src.txt host:5031/dst
./bin/gcp cp --dry-run 'host:5031/dir/*.txt' ./local/
```

### Reliability options

```
./bin/gcp cp --timeout 15s --retry 3 --checksum true ./src.txt host:5031/dst
```

### `cp` flags

| Flag              | Default | Description                            |
|-------------------|---------|----------------------------------------|
| `--chunk`         | 32768   | chunk size in bytes                    |
| `--batch`         | 4       | max concurrent chunk transfers         |
| `--timeout`       | 15s     | read/write timeout (0 to disable)      |
| `--retry`         | 3       | max chunk transfer retries             |
| `--checksum`      | true    | enable CRC-32 chunk integrity checks   |
| `--recursive, -r` | false   | copy directories recursively           |
| `--dry-run`       | false   | show transfer plan without doing it    |
| `--level, -L`     | error   | log level (info, debug, etc.)          |

## Protocol safety limits

The asyncio protocol reads `msgSize` and `payloadSize` from the wire header. Two limits in `asyncio/message.go` prevent abuse:

- **`MaxMsgSize` = 64 KB** — CBOR-encoded messages are tiny structs (ID, ints, path string). 64 KB is several orders of magnitude above any legitimate message, but far below anything dangerous.
- **`MaxPayloadSize` = 16 MB** — File chunks default to 32 KB (`--chunk`). 16 MB allows very large chunks while still being small enough that a single allocation won't OOM the process. 64 concurrent 16 MB allocations would be needed to hit 1 GB.

Without these two bounds a malicious or corrupted header could set `msgSize = 0xFFFFFFFF` to cause a slice-bounds panic or `payloadSize = math.MaxUint32` to trigger a 4 GB `make` OOM.

## Architecture

- `cmd/asyncio/` — primary CLI entrypoint and copy logic
- `asyncio/` — shared protocol library (message types, CBOR encode/decode, wire format)
- `logger/` — zerolog wrapped as `slog.Logger`
- `cmd/progressbar/` — real-time progress display using `go-humanize`
### Message types

The custom TCP protocol uses a binary header with magic bytes (`0xA8 0xD5`), message type, and CBOR-encoded payloads:

| Message       | Direction       | Description                    |
|---------------|-----------------|--------------------------------|
| `CreateReq`   | client → server | create file or directory       |
| `CreateRes`   | server → client | create result                  |
| `WriteReq`    | client → server | write chunk at offset          |
| `WriteRes`    | server → client | write result                   |
| `ReadReq`     | client → server | read chunk at offset           |
| `ReadRes`     | server → client | read result + chunk data       |
| `StatReq`     | client → server | get file metadata              |
| `StatRes`     | server → client | file size, mode, type          |
| `ReadDirReq`  | client → server | list directory entries         |
| `ReadDirRes`  | server → client | entry names, types, modes      |
