package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/beeleelee/gcp/asyncio"
	"github.com/beeleelee/gcp/cmd/progressbar"
	"github.com/beeleelee/gcp/logger"
)

// resumeState tracks which chunks of a file transfer have been confirmed by
// the receiver. This enables breakpoint resume across process restarts.
//
// The in-memory completedSet (map) provides O(1) lookups during chunk
// scheduling, while Completed (slice) is the JSON-serialized representation
// persisted to disk. Pending collects newly-completed offsets for batched
// writes, reducing disk I/O compared to writing on every chunk.
type resumeState struct {
	Version    int     `json:"version"`
	SourceSize int64   `json:"source_size"`
	ChunkSize  int64   `json:"chunk_size"`
	Completed  []int64 `json:"completed"`

	path         string
	completedSet map[int64]struct{}
	pending      []int64
	batch        int
	mu           sync.Mutex
}

// stateFilePath derives a deterministic, collision-resistant path for the
// resume state file. The filename is SHA-256(128 bits) of source:target,
// stored under os.TempDir() + "/gcp/".
func stateFilePath(srcPath, targetPath string) string {
	h := sha256.Sum256([]byte(srcPath + ":" + targetPath))
	name := fmt.Sprintf("gcp-resume-%s.json", hex.EncodeToString(h[:16]))
	return filepath.Join(os.TempDir(), "gcp", name)
}

// loadResumeState reads a previously saved state file. It returns nil if
// the file does not exist, is corrupt, or the source size or chunk size
// no longer match the current transfer (indicating the file has changed).
func loadResumeState(srcPath, targetPath string, sourceSize, chunkSize int64, batch int) *resumeState {
	p := stateFilePath(srcPath, targetPath)
	f, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer f.Close()

	var s resumeState
	if err := json.NewDecoder(f).Decode(&s); err != nil {
		return nil
	}
	if s.Version != 1 || s.SourceSize != sourceSize || s.ChunkSize != chunkSize {
		return nil
	}

	s.path = p
	s.batch = batch
	s.completedSet = make(map[int64]struct{}, len(s.Completed))
	for _, off := range s.Completed {
		s.completedSet[off] = struct{}{}
	}
	return &s
}

// saveResumeState writes the completed-set bitmap to disk atomically: it
// writes to a .tmp file, then renames over the target. This prevents
// torn-writes from corrupting the state across a crash.
func saveResumeState(s *resumeState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	s.Completed = make([]int64, 0, len(s.completedSet))
	for off := range s.completedSet {
		s.Completed = append(s.Completed, off)
	}
	sort.Slice(s.Completed, func(i, j int) bool { return s.Completed[i] < s.Completed[j] })

	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(s); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, s.path)
}

// deleteResumeState removes the state file on disk. It is called when a
// transfer completes successfully, so that a future identical transfer
// does not skip zero-length or other boundary-case chunks.
func deleteResumeState(s *resumeState) {
	if s == nil {
		return
	}
	os.Remove(s.path)
}

// addCompletedOffset marks a chunk offset as completed. It buffers the
// update in s.pending and flushes to disk only when the batch threshold
// is reached, reducing disk I/O without losing too much progress on crash.
func addCompletedOffset(s *resumeState, offset int64) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.completedSet[offset] = struct{}{}
	s.pending = append(s.pending, offset)
	shouldFlush := len(s.pending) >= s.batch
	s.mu.Unlock()

	if shouldFlush {
		return saveResumeState(s)
	}
	return nil
}

// flushResumeState forces a disk write of any buffered (pending)
// completions. This is called after all chunks are dispatched to ensure
// the final state is durable before post-transfer verification.
func flushResumeState(s *resumeState) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	hasPending := len(s.pending) > 0
	s.mu.Unlock()
	if hasPending {
		return saveResumeState(s)
	}
	return nil
}

// isCompleted checks whether a given chunk offset was already confirmed
// by the receiver. It is used during chunk scheduling to skip chunks that
// were already transferred in a prior run.
func isCompleted(s *resumeState, offset int64) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	_, ok := s.completedSet[offset]
	s.mu.Unlock()
	return ok
}

// processChunks divides a file into fixed-size chunks and dispatches them
// concurrently to fn, bounded by a semaphore of size batch. It supports
// resume by skipping already-completed offsets. Progress is reported via
// progressChan, and the first error from any goroutine cancels all other
// in-flight chunks. Returns nil when all chunks succeed.
func processChunks(
	ctx context.Context,
	fileSize, chunkSize int64,
	startOffset int64,
	batch int,
	state *resumeState,
	fn func(context.Context, int64, int64, chan<- int64) error,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	sem := make(chan struct{}, batch)
	errChan := make(chan error, batch)
	progressChan := make(chan int64, batch+1)
	go progressbar.Progress(ctx, fileSize, progressChan, time.Now(), time.Millisecond*200)
	progressChan <- startOffset

	offset := startOffset
	remainSize := fileSize - startOffset

	for remainSize > 0 {
		select {
		case err := <-errChan:
			cancel()
			return err
		default:
		}
		size := chunkSize
		if remainSize < size {
			size = remainSize
		}

		if isCompleted(state, offset) {
			progressChan <- size
			offset += size
			remainSize -= size
			continue
		}

		wg.Add(1)
		go func(off, sz int64) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			if err := fn(ctx, off, sz, progressChan); err != nil {
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
			}
		}(offset, size)
		offset += size
		remainSize -= size
	}
	wg.Wait()

	if err := flushResumeState(state); err != nil {
		logger.Log.Debug("failed to flush resume state", "err", err)
	}

	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// verifyFileHash requests a SHA-256 hash of the remote file from the
// server, computes the local file's SHA-256, and compares them. A mismatch
// is returned as an error. This is used for post-transfer integrity checks
// controlled by the --sha256 flag.
func verifyFileHash(ctx context.Context, cc *copierClient, remotePath, localPath string) error {
	logger.Log.Debug("verifying file hash", "remote", remotePath, "local", localPath)

	res, err := cc.Hash(remotePath)
	if err != nil {
		return fmt.Errorf("hash request failed: %w", err)
	}
	hashRes := res.msg.(*asyncio.HashRes)
	if !hashRes.Success {
		return fmt.Errorf("hash failed: %s", hashRes.Error)
	}

	fd, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("cannot open local file for hash: %w", err)
	}
	defer fd.Close()

	h := sha256.New()
	if _, err := io.Copy(h, fd); err != nil {
		return fmt.Errorf("cannot read local file for hash: %w", err)
	}
	localHash := h.Sum(nil)

	if len(hashRes.Hash) != len(localHash) {
		return fmt.Errorf("hash length mismatch: server=%d local=%d", len(hashRes.Hash), len(localHash))
	}
	for i := range localHash {
		if hashRes.Hash[i] != localHash[i] {
			return fmt.Errorf("SHA-256 mismatch: file may be corrupted")
		}
	}

	logger.Log.Debug("hash verification passed")
	return nil
}

// compressChunk compresses data using the algorithm indicated by algo
// (CompressionGzip, etc.). If the compressed result is not smaller than
// the original, the original is returned as-is (auto-skip).
func compressChunk(data []byte, algo uint8) ([]byte, uint8, error) {
	if algo == asyncio.CompressionNone || len(data) == 0 {
		return data, asyncio.CompressionNone, nil
	}
	switch algo {
	case asyncio.CompressionGzip:
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
		if _, err := zw.Write(data); err != nil {
			return nil, asyncio.CompressionNone, err
		}
		if err := zw.Close(); err != nil {
			return nil, asyncio.CompressionNone, err
		}
		compressed := buf.Bytes()
		if len(compressed) >= len(data) {
			return data, asyncio.CompressionNone, nil
		}
		return compressed, algo, nil
	default:
		return data, asyncio.CompressionNone, nil
	}
}

// decompressChunk decompresses data using the algorithm indicated by algo.
// If algo is CompressionNone the data is returned verbatim.
func decompressChunk(data []byte, algo uint8) ([]byte, error) {
	if algo == asyncio.CompressionNone || len(data) == 0 {
		return data, nil
	}
	switch algo {
	case asyncio.CompressionGzip:
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	default:
		return data, nil
	}
}
