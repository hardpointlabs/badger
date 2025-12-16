package table

import (
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/ristretto/v2/z"
)


type HybridMmapFile struct {
	Data []byte
	Fd   *os.File
}

func fromRistretto(mmf *z.MmapFile) (*HybridMmapFile) {
	return &HybridMmapFile{
		mmf.Data,
		mmf.Fd,
	}
}

func OpenMmapFile(filename string, flag int, maxSz int) (*HybridMmapFile, error) {
	if (strings.HasPrefix("s3://", filename)) {

	}
	mmf, err := z.OpenMmapFile(filename, flag, maxSz)
	if err != nil {
		return nil, err
	}
	return fromRistretto(mmf), nil
}

func (m *HybridMmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return z.Msync(m.Data)
}

// Close would close the file. It would also truncate the file if maxSz >= 0.
func (m *HybridMmapFile) Close(maxSz int64) error {
	// Badger can set the m.Data directly, without setting any Fd. In that case, this should be a
	// NOOP.
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := z.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if maxSz >= 0 {
		if err := m.Fd.Truncate(maxSz); err != nil {
			return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
		}
	}
	return m.Fd.Close()
}

func (m *HybridMmapFile) Delete() error {
	// Badger can set the m.Data directly, without setting any Fd. In that case, this should be a
	// NOOP.
	if m.Fd == nil {
		return nil
	}

	if err := z.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}