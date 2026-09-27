package cas

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// 本文件负责数据块（blob）与临时文件的落盘细节：
//   - blobs/<摘要前2字节>/<完整摘要>：不可变内容块；
//   - tmp/<随机名>：写入中的临时文件，崩溃重启后统一清扫；
//   - 所有落盘操作都遵循“写临时文件 -> fsync -> 原子 rename -> fsync 目录”。
//

// blobPath 返回摘要 d 对应的数据块路径。
func blobPath(dir string, d Digest) string {
	return filepath.Join(dir, "blobs", string(d[:2]), string(d))
}

// tempPath 返回临时目录中的一个候选路径。
func tempPath(dir string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("cas: generate temp name: %w", err)
	}
	name := "w-" + hex.EncodeToString(b[:]) + ".tmp"
	return filepath.Join(dir, "tmp", name), nil
}

// writeTemp 将 data 完整写入临时目录并做 fsync，返回临时文件路径。
// 调用方之后负责把它原子地安装为正式数据块，或在放弃时删除它。
func writeTemp(dir string, data []byte) (path string, err error) {
	path, err = tempPath(dir)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("cas: create temp file: %w", err)
	}
	defer func() {
		// 出错或放弃安装时尽力清理临时文件；残留也会在 Open/GC 时被清扫。
		if err != nil {
			f.Close()
			os.Remove(path)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return "", fmt.Errorf("cas: write temp file: %w", err)
	}
	if err = f.Sync(); err != nil {
		return "", fmt.Errorf("cas: fsync temp file: %w", err)
	}
	if err = f.Close(); err != nil {
		return "", fmt.Errorf("cas: close temp file: %w", err)
	}
	return path, nil
}

// installBlob 在确认目标不存在后，把临时文件原子改名为数据块；
// 若目标已存在（并发写入的胜出者已安装），则删除本次临时文件并返回 installed=false。
//
// 这里用 rename 而非 link：rename 是原子的，且同进程内由排他锁串行化，
// 跨进程则由 flock 排他锁串行化；双重保险下不会覆盖既有内容块。
func installBlob(dir string, d Digest, tmp string) (bool, error) {
	target := blobPath(dir, d)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return false, fmt.Errorf("cas: create blob shard dir: %w", err)
	}
	if _, err := os.Stat(target); err == nil {
		// 数据块已存在（去重命中 / 并发写入的胜出者），放弃临时文件。
		if rmErr := os.Remove(tmp); rmErr != nil && !os.IsNotExist(rmErr) {
			return false, fmt.Errorf("cas: remove duplicate temp: %w", rmErr)
		}
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("cas: stat blob: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		return false, fmt.Errorf("cas: install blob rename: %w", err)
	}
	// fsync 分片目录，保证 rename 持久化。
	if err := fsyncDir(filepath.Dir(target)); err != nil {
		return false, err
	}
	return true, nil
}

// readBlob 读取数据块的全部字节及其文件信息。
func readBlob(dir string, d Digest) ([]byte, os.FileInfo, error) {
	path := blobPath(dir, d)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("%w: blob %s", ErrNotFound, d)
		}
		return nil, nil, fmt.Errorf("cas: open blob %s: %w", d, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("cas: stat blob %s: %w", d, err)
	}
	data := make([]byte, info.Size())
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, nil, fmt.Errorf("cas: read blob %s: %w", d, err)
	}
	return data, info, nil
}

// digestData 返回 data 的 SHA-256 摘要。
func digestData(data []byte) Digest {
	sum := sha256.Sum256(data)
	return Digest(hex.EncodeToString(sum[:]))
}

// fsyncDir 对目录执行 fsync，确保其中的 rename/create 已持久化。
func fsyncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cas: open dir %q for fsync: %w", path, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("cas: fsync dir %q: %w", path, err)
	}
	return nil
}
