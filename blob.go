package castore

// 本文件负责数据块（blob）的暂存、发布与校验。
// blob 以其 SHA-256 摘要作为寻址键，文件一经发布即不可变；
// 发布通过 "写临时文件 + fsync + 同文件系统原子 rename" 完成，
// 因此崩溃后磁盘上只可能出现完整 blob，绝无半个 blob。

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
)

// 磁盘布局中的固定目录/文件名。
const (
	blobsDirName = "blobs"
	tmpDirName   = "tmp"
	metaFileName = "meta.json"
)

// blobWriter 把一份对象内容写入暂存区，并在写入过程中同步计算摘要。
// 只有调用 commit 成功发布后，暂存文件才会成为正式 blob；
// 进程在 commit 之前退出留下的暂存文件会在下次 Open 时被清理，
// 绝不会被当成任何版本的数据。
type blobWriter struct {
	f    *os.File
	h    hash.Hash
	size int64
	done bool
}

// newBlobWriter 在 root/tmp 下创建一个唯一命名的暂存文件。
func newBlobWriter(root string) (*blobWriter, error) {
	if err := os.MkdirAll(filepath.Join(root, tmpDirName), 0o755); err != nil {
		return nil, fmt.Errorf("castore: create tmp dir: %w", err)
	}
	f, err := os.CreateTemp(filepath.Join(root, tmpDirName), "ingest-*")
	if err != nil {
		return nil, fmt.Errorf("castore: create temp blob: %w", err)
	}
	return &blobWriter{f: f, h: sha256.New()}, nil
}

// Write 写入对象字节，同时累加摘要与长度。
func (w *blobWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if n > 0 {
		w.h.Write(p[:n])
		w.size += int64(n)
	}
	return n, err
}

// Name 返回暂存文件路径，供失败路径清理。
func (w *blobWriter) Name() string { return w.f.Name() }

// commit 持久化暂存内容并原子发布为正式 blob。
// 返回内容的十六进制 SHA-256 摘要与字节数。
// 相同内容的并发提交最终 rename 到同一目标路径，POSIX rename 原子，
// 且双方字节完全一致，因此只会保留一份正确数据。
func (w *blobWriter) commit(root string) (string, int64, error) {
	if w.done {
		return "", 0, errors.New("castore: blob writer already committed")
	}
	hash := hex.EncodeToString(w.h.Sum(nil))

	// 1) 数据落盘且持久化。
	if err := w.f.Sync(); err != nil {
		return "", 0, fmt.Errorf("castore: sync temp blob: %w", err)
	}
	tmpName := w.f.Name()
	if err := w.f.Close(); err != nil {
		return "", 0, fmt.Errorf("castore: close temp blob: %w", err)
	}
	w.done = true

	final := blobPath(root, hash)

	// 2) 目标已存在说明相同内容之前已发布（去重命中），丢弃本次暂存即可。
	if _, err := os.Stat(final); err == nil {
		if err := os.Remove(tmpName); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", 0, fmt.Errorf("castore: remove duplicate temp blob: %w", err)
		}
		return hash, w.size, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", 0, fmt.Errorf("castore: stat blob: %w", err)
	}

	// 3) 原子发布到 blobs/xx/<hash>。
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return "", 0, fmt.Errorf("castore: create shard dir: %w", err)
	}
	if err := os.Rename(tmpName, final); err != nil {
		return "", 0, fmt.Errorf("castore: publish blob: %w", err)
	}
	if err := fsyncDir(filepath.Dir(final)); err != nil {
		return "", 0, err
	}
	return hash, w.size, nil
}

// abort 放弃暂存（commit 失败或元数据提交失败时调用）。
func (w *blobWriter) abort() {
	if !w.done {
		_ = w.f.Close()
	}
	_ = os.Remove(w.f.Name())
}

// blobPath 返回某摘要对应 blob 的落盘路径：按摘要前两位分桶。
func blobPath(root, hash string) string {
	return filepath.Join(root, blobsDirName, hash[:2], hash)
}

// readBlob 读取 blob 全部字节，返回前重新计算摘要并与寻址键比对，
// 任何不一致都返回 ErrCorrupt，杜绝脏数据流出。
func readBlob(root, hash string) ([]byte, error) {
	f, err := os.Open(blobPath(root, hash))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: blob %s", ErrNotFound, hash)
		}
		return nil, fmt.Errorf("castore: open blob: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	data, err := io.ReadAll(io.TeeReader(f, h))
	if err != nil {
		return nil, fmt.Errorf("castore: read blob: %w", err)
	}
	if hex.EncodeToString(h.Sum(nil)) != hash {
		return nil, fmt.Errorf("%w: blob %s checksum mismatch", ErrCorrupt, hash)
	}
	return data, nil
}

// blobSize 返回 blob 的字节数。
func blobSize(root, hash string) (int64, error) {
	fi, err := os.Stat(blobPath(root, hash))
	if err != nil {
		return 0, fmt.Errorf("castore: stat blob: %w", err)
	}
	return fi.Size(), nil
}

// removeBlob 物理删除一个 blob。
func removeBlob(root, hash string) error {
	if err := os.Remove(blobPath(root, hash)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("castore: remove blob: %w", err)
	}
	return nil
}

// cleanupTempBlobs 清理 tmp 目录下所有遗留暂存文件（中断恢复）。
// 暂存文件永远不会被元数据引用，直接整体清空是安全的。
func cleanupTempBlobs(root string) error {
	tmp := filepath.Join(root, tmpDirName)
	entries, err := os.ReadDir(tmp)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("castore: scan tmp dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(tmp, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("castore: remove stale temp %s: %w", e.Name(), err)
		}
	}
	return nil
}

// fsyncDir fsync 一个目录，保证其内的 rename/create 元数据已持久化。
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("castore: open dir for fsync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("castore: fsync dir %s: %w", dir, err)
	}
	return nil
}
