// Package cas 实现了一个基于本地磁盘的内容寻址对象存储（Content-Addressed Store）。
//
// 核心能力：
//   - 按内容哈希寻址，相同内容只落盘一份数据，天然去重；
//   - 对象（key）拥有多个不可变版本，每个版本引用一个内容块（blob）；
//   - 引用计数驱动的垃圾回收，只回收无任何版本引用的数据块；
//   - 写入走临时文件 + 原子改名，崩溃重启后未完成写入不会成为有效版本；
//   - 通过文件锁（flock）协调同进程与跨进程的并发访问。
package cas

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Digest 是内容块的 SHA-256 十六进制摘要。
type Digest string

// 包级哨兵错误。
var (
	// ErrNotFound 表示对象或版本不存在。
	ErrNotFound = errors.New("cas: object or version not found")
	// ErrChecksum 表示读取到的数据块内容与其摘要不一致（数据已损坏或被篡改）。
	ErrChecksum = errors.New("cas: blob content checksum mismatch")
)

// Version 描述某个对象的一个不可变版本。
type Version struct {
	// ID 是存储内全局单调递增的版本号。
	ID int `json:"id"`
	// Key 是对象名（逻辑路径）。
	Key string `json:"key"`
	// Digest 指向该版本内容的数据块摘要。
	Digest Digest `json:"digest"`
	// Size 是内容字节数。
	Size int64 `json:"size"`
	// CreatedAt 是版本创建时间（UTC）。
	CreatedAt time.Time `json:"created_at"`
}

// GCReport 汇总一次垃圾回收的结果。
type GCReport struct {
	// Removed 是被回收的数据块摘要。
	Removed []Digest
	// BytesFreed 是回收释放的字节数。
	BytesFreed int64
	// SweptTempFiles 是启动/回收时清扫掉的遗留临时文件。
	SweptTempFiles []string
}

// LoggerFunc 用于接收存储内部的可解释性日志（摘要、引用计数变化等）。
type LoggerFunc func(format string, args ...any)

// Store 是本地内容寻址存储的句柄。
type Store struct {
	dir  string
	logf LoggerFunc

	mu    sync.RWMutex // 同进程 goroutine 间的读写锁
	flock *fileLock    // 跨进程文件锁：读者共享、写者/GC 排他
}

// Open 打开（必要时创建）一个位于 dir 的存储实例。
// 打开时会在排他锁保护下：清扫遗留临时文件（中断写入的痕迹）、
// 以版本链为准重算引用计数，确保崩溃后恢复到一致状态。
func Open(dir string) (*Store, error) {
	for _, sub := range []string{"blobs", "tmp", "meta"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, fmt.Errorf("cas: init dir %q: %w", sub, err)
		}
	}
	fl, err := newFileLock(filepath.Join(dir, "lock"))
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:   dir,
		logf:  func(string, ...any) {},
		flock: fl,
	}
	// 启动恢复：排他锁内清扫 tmp 并修复引用计数。
	// 还没有任何外部使用者，不需要再取 s.mu。
	if err := fl.lock(); err != nil {
		fl.close()
		return nil, err
	}
	if err := s.recoverLocked(); err != nil {
		fl.unlock()
		fl.close()
		return nil, err
	}
	if err := fl.unlock(); err != nil {
		fl.close()
		return nil, err
	}
	return s, nil
}

// WithLogger 设置内部日志回调（如测试中打印内容摘要与引用计数变化），返回 Store 自身以便链式调用。
func (s *Store) WithLogger(f LoggerFunc) *Store {
	if f == nil {
		f = func(string, ...any) {}
	}
	s.logf = f
	return s
}

// Close 释放存储持有的资源。
func (s *Store) Close() error {
	return s.flock.close()
}

// recoverLocked 必须在排他锁内调用：清扫临时文件并重算引用计数。
func (s *Store) recoverLocked() error {
	swept, err := sweepTemp(s.dir)
	if err != nil {
		return err
	}
	for _, p := range swept {
		s.logf("[recover] sweep incomplete write temp file: %s", filepath.Base(p))
	}

	m, err := loadManifest(s.dir)
	if err != nil {
		return err
	}
	changed := m.reconcileRefs()
	if len(changed) > 0 {
		if err := saveManifest(s.dir, m); err != nil {
			return err
		}
		digests := make([]string, 0, len(changed))
		for d := range changed {
			digests = append(digests, d)
		}
		sort.Strings(digests)
		for _, d := range digests {
			s.logf("[recover] refcount fixed after restart: digest=%s refs=%d", d, changed[d])
		}
	}
	s.logf("[recover] store opened: %d objects, %d referenced blobs, %d temp files swept",
		len(m.Keys), len(m.Refs), len(swept))
	return nil
}

// sweepTemp 删除 tmp/ 下的所有文件，返回被删除文件的完整路径。
// tmp/ 中只可能有“写入中”的文件；持有排他锁时没有写入正在进行，
// 因此这些都是进程中断留下的残留，绝不会是有效版本。
func sweepTemp(dir string) ([]string, error) {
	tmpDir := filepath.Join(dir, "tmp")
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cas: read tmp dir: %w", err)
	}
	var removed []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(tmpDir, e.Name())
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("cas: remove temp file %q: %w", p, err)
		}
		removed = append(removed, p)
	}
	return removed, nil
}

// Put 将 data 作为 key 的一个新版本写入，返回该版本描述。
// 相同内容在磁盘上只会存在一份数据块，但每次上传都会形成一个引用该数据块的版本。
//
// 持久化顺序保证崩溃一致性：
//  1. 先把内容写入 tmp/ 临时文件并 fsync；
//  2. 持排他锁，将临时文件原子安装为 blobs/<digest>（去重：已存在则丢弃临时文件）；
//  3. 最后才把版本与引用计数写入清单并原子提交。
//
// 任何一步崩溃：清单未提交 => 没有任何新版本；遗留临时文件由下次 Open 清扫。
func (s *Store) Put(key string, data []byte) (*Version, error) {
	d := digestData(data)
	size := int64(len(data))

	// 内容计算与临时文件写入不需要锁；并发写相同内容时各自写各自的临时文件，
	// 安装阶段才在锁内决出唯一胜出者，败者的临时文件被立即删除。
	tmp, err := writeTemp(s.dir, data)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flock.lock(); err != nil {
		return nil, err
	}
	defer s.flock.unlock()

	m, err := loadManifest(s.dir)
	if err != nil {
		return nil, err
	}
	installed, err := installBlob(s.dir, d, tmp)
	if err != nil {
		return nil, err
	}

	// 同一 key 的相同内容：视为重复上传，直接返回既有版本，不新增版本、不重复计数。
	if v, ok := findVersionByDigest(m, key, d); ok {
		s.logf("[put] duplicate upload key=%q digest=%s -> existing version v%d (refs=%d, installed=%v)",
			key, d, v.ID, m.refCount(d), installed)
		ver := v.toVersion(key)
		return &ver, nil
	}

	sv := storedVersion{
		ID:        m.NextID,
		Digest:    d,
		Size:      size,
		CreatedAt: time.Now().UTC().UnixNano(),
	}
	m.Keys[key] = append(m.Keys[key], sv)
	m.NextID++
	old := m.refCount(d)
	m.Refs[string(d)] = old + 1
	if err := saveManifest(s.dir, m); err != nil {
		return nil, err
	}
	ver := sv.toVersion(key)
	s.logf("[put] key=%q digest=%s size=%d installed=%v -> new version v%d, refs %d -> %d",
		key, d, size, installed, ver.ID, old, old+1)
	return &ver, nil
}

// Get 读取 key 最新版本的原始字节，返回前会校验内容摘要。
func (s *Store) Get(key string) ([]byte, *Version, error) {
	return s.getVersion(key, 0) // id=0 表示最新版本
}

// GetVersion 读取 key 指定版本的原始字节，返回前会校验内容摘要。
func (s *Store) GetVersion(key string, id int) ([]byte, *Version, error) {
	return s.getVersion(key, id)
}

// getVersion 的读取窗口：持有共享锁读出全部字节并校验，
// 校验通过后才释放锁返回副本，因此 GC 不可能在读取中途删除数据块
// （GC 需要排他锁，会等所有读者结束）。
func (s *Store) getVersion(key string, id int) ([]byte, *Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.flock.rlock(); err != nil {
		return nil, nil, err
	}
	defer s.flock.unlock()

	m, err := loadManifest(s.dir)
	if err != nil {
		return nil, nil, err
	}
	versions, ok := m.Keys[key]
	if !ok || len(versions) == 0 {
		return nil, nil, fmt.Errorf("%w: key %q", ErrNotFound, key)
	}
	var sv storedVersion
	switch {
	case id == 0:
		sv = versions[len(versions)-1]
	default:
		var found bool
		for _, v := range versions {
			if v.ID == id {
				sv, found = v, true
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("%w: key %q version v%d", ErrNotFound, key, id)
		}
	}

	data, _, err := readBlob(s.dir, sv.Digest)
	if err != nil {
		return nil, nil, err
	}
	// 内容完整性校验：实际哈希必须等于版本记录的摘要。
	if got := digestData(data); got != sv.Digest {
		s.logf("[get] CHECKSUM FAILURE key=%q v%d expected=%s actual=%s", key, sv.ID, sv.Digest, got)
		return nil, nil, fmt.Errorf("%w: key %q v%d: expected %s, got %s",
			ErrChecksum, key, sv.ID, sv.Digest, got)
	}
	ver := sv.toVersion(key)
	s.logf("[get] key=%q v%d digest=%s size=%d verified ok, refs=%d",
		key, ver.ID, ver.Digest, ver.Size, m.refCount(ver.Digest))
	return data, &ver, nil
}

// ListVersions 按版本号升序列出 key 的全部版本。
func (s *Store) ListVersions(key string) ([]Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.flock.rlock(); err != nil {
		return nil, err
	}
	defer s.flock.unlock()

	m, err := loadManifest(s.dir)
	if err != nil {
		return nil, err
	}
	stored, ok := m.Keys[key]
	if !ok {
		return nil, fmt.Errorf("%w: key %q", ErrNotFound, key)
	}
	out := make([]Version, len(stored))
	for i, sv := range stored {
		out[i] = sv.toVersion(key)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// DeleteVersion 删除 key 的指定版本（解除其对数据块的引用）。
// 数据块本身不会立即被删除，而是等待 GC 在引用计数归零时回收。
func (s *Store) DeleteVersion(key string, id int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flock.lock(); err != nil {
		return err
	}
	defer s.flock.unlock()

	m, err := loadManifest(s.dir)
	if err != nil {
		return err
	}
	versions, ok := m.Keys[key]
	if !ok {
		return fmt.Errorf("%w: key %q", ErrNotFound, key)
	}
	idx := -1
	for i, v := range versions {
		if v.ID == id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("%w: key %q version v%d", ErrNotFound, key, id)
	}
	d := versions[idx].Digest
	m.Keys[key] = append(versions[:idx], versions[idx+1:]...)
	if len(m.Keys[key]) == 0 {
		delete(m.Keys, key) // 保持清单整洁
	}
	old := m.refCount(d)
	if old <= 1 {
		delete(m.Refs, string(d)) // 归零：等待 GC
	} else {
		m.Refs[string(d)] = old - 1
	}
	if err := saveManifest(s.dir, m); err != nil {
		return err
	}
	now := m.refCount(d)
	s.logf("[delete] key=%q v%d digest=%s refs %d -> %d (blob retained until GC: %v)",
		key, id, d, old, now, now > 0)
	return nil
}

// GC 回收没有任何版本引用且未被读取中的数据块，并清扫遗留临时文件。
// 排他锁保证：没有任何版本变更、也没有任何共享锁读取者（跨进程同样如此）。
func (s *Store) GC() (*GCReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flock.lock(); err != nil {
		return nil, err
	}
	defer s.flock.unlock()

	report := &GCReport{}

	// 先清扫中断写入的残留临时文件。
	swept, err := sweepTemp(s.dir)
	if err != nil {
		return nil, err
	}
	report.SweptTempFiles = swept
	for _, p := range swept {
		s.logf("[gc] sweep incomplete write temp file: %s", filepath.Base(p))
	}

	m, err := loadManifest(s.dir)
	if err != nil {
		return nil, err
	}
	// 以版本链为准重算引用计数（正常路径下不会有变化，这里是防御性修复）。
	changed := m.reconcileRefs()
	for d, n := range changed {
		s.logf("[gc] refcount reconciled: digest=%s refs=%d", d, n)
	}

	blobsDir := filepath.Join(s.dir, "blobs")
	shardEntries, err := os.ReadDir(blobsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return report, nil
		}
		return nil, fmt.Errorf("cas: read blobs dir: %w", err)
	}
	for _, shard := range shardEntries {
		if !shard.IsDir() {
			continue
		}
		shardDir := filepath.Join(blobsDir, shard.Name())
		files, err := os.ReadDir(shardDir)
		if err != nil {
			return nil, fmt.Errorf("cas: read blob shard: %w", err)
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			d := Digest(f.Name())
			if n := m.refCount(d); n > 0 {
				// 仍有版本引用：绝不回收。
				s.logf("[gc] retain digest=%s refs=%d", d, n)
				continue
			}
			info, err := f.Info()
			if err != nil {
				return nil, fmt.Errorf("cas: stat blob %s: %w", d, err)
			}
			if err := os.Remove(filepath.Join(shardDir, f.Name())); err != nil {
				return nil, fmt.Errorf("cas: remove unreferenced blob %s: %w", d, err)
			}
			report.Removed = append(report.Removed, d)
			report.BytesFreed += info.Size()
			s.logf("[gc] collect unreferenced digest=%s bytes=%d refs=0", d, info.Size())
		}
	}
	sort.Slice(report.Removed, func(i, j int) bool { return report.Removed[i] < report.Removed[j] })
	if len(changed) > 0 {
		if err := saveManifest(s.dir, m); err != nil {
			return nil, err
		}
	}
	s.logf("[gc] done: %d blobs collected, %d bytes freed, %d temp files swept",
		len(report.Removed), report.BytesFreed, len(report.SweptTempFiles))
	return report, nil
}

// findVersionByDigest 在 key 的版本链中查找引用摘要 d 的版本。
func findVersionByDigest(m *manifest, key string, d Digest) (storedVersion, bool) {
	for _, v := range m.Keys[key] {
		if v.Digest == d {
			return v, true
		}
	}
	return storedVersion{}, false
}
