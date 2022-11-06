package progress

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"sync/atomic"
)

type ReadHasher struct {
	r      io.Reader
	md5    hash.Hash
	sha256 hash.Hash
}

func NewReadHasher(r io.Reader) *ReadHasher {
	return &ReadHasher{
		r:      r,
		md5:    md5.New(),
		sha256: sha256.New(),
	}
}

func (h *ReadHasher) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		// hashers never return errors
		h.md5.Write(p[:n])
		h.sha256.Write(p[:n])
	}
	return n, err
}

func (h *ReadHasher) FinishReader() (int64, error) {
	return io.Copy(io.MultiWriter(h.md5, h.sha256), h.r)
}

func (h *ReadHasher) MD5() string {
	return hex.EncodeToString(h.md5.Sum(nil))
}

func (h *ReadHasher) SHA256() string {
	return hex.EncodeToString(h.sha256.Sum(nil))
}

type ReadCounter struct {
	r     io.Reader
	count uint64
}

func NewReadCounter(r io.Reader) *ReadCounter {
	return &ReadCounter{r: r}
}

func (h *ReadCounter) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		atomic.AddUint64(&h.count, uint64(n))
	}
	return n, err
}

func (h *ReadCounter) Count() uint64 {
	return atomic.LoadUint64(&h.count)
}
