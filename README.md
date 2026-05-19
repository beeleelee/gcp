# gcp

Copy files from local to a remote host over TCP. Two implementations in one repo.

## Binaries

| make target | source dir     | output binary   | transport          |
|-------------|----------------|-----------------|--------------------|
| `gcp`       | `cmd/asyncio/` | `./bin/gcp`     | custom TCP + CBOR  |
| `blockio`   | `cmd/blockio/` | `./bin/blockio` | gRPC               |

Both default to port `1717`.

## Build

```
make gcp        # asyncio binary
make blockio    # gRPC binary
```

## Usage

### Serve (on remote host)

```
./bin/gcp serve --listen tcp://0.0.0.0:1717 --process-num 4 --multicore true
./bin/blockio serve --listen :1717
```

### Copy (from local)

```
./bin/gcp cp --host localhost:1717 ./src.txt /remote/path/dst.txt
./bin/blockio cp --host localhost:1717 --chunk 32768 --batch 16 ./src.txt /remote/path/dst.txt
```

The target path is optional — if omitted or empty, the source filename is used:

```
./bin/gcp cp --host localhost:1717 ./src.txt
# copies to <remote working dir>/src.txt
```

If the target ends with `/`, the source filename is appended:

```
./bin/gcp cp --host localhost:1717 ./src.txt /remote/dir/
# copies to /remote/dir/src.txt
```

### Flags

| Flag     | Default        | Description           |
|----------|----------------|-----------------------|
| `--host` | `localhost:1717` | Remote host address |
| `--chunk`| `32768`        | Chunk size in bytes   |
| `--batch`| `4` (gcp) / `16` (blockio) | Max concurrent chunks |

Log level: `--level` or `-L` flag (`error` default, accepts `info`, `debug`, etc.).

