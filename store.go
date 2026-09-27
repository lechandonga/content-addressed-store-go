package castore

// 本文件是存储门面：协调 blob、元数据与垃圾回收。
//
// 一致性策略：
//   - mu 为读写锁：Put/Delete/GC 取写锁串行提交元数据快照；
//     Get/Versions/RefCount 取读锁。
//   - 每次写入先把 blob 以内容摘要为名原子发布（天然去重），
//     再在一个元数据快照里登记版本并递增引用计数；
//     元数据提交失败则整体回滚内存状态，blob 即使残留也只是
//     引用计数为 0 的孤儿，后续 GC 回收。
//   - Get 在持读锁期间把目标 blob 登记到 reads，随后释放读锁再读文件；
//     GC 持写锁时，任何可能进行中的读取都一定体现在 reads 中，
//     因此 GC 绝不会误删正在被读取的数据。

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Version 描述对象的一个不可变版本。
type Version struct {
	Object  string `json:"object"`
	Version uint64 `json:"version"`
	Hash    string `json:"hash"`
	Size    int64  `json:"size"`
}

// Store 是本地内容寻址对象存储。
type Store struct {
	root string

	mu   sync.RWMutex
	meta *metaData

	// reads 记录正在读取中的 blob 摘要 -> 进行中的读取数。
	// GC 必须跳过计数非零的 blob。
	readsMu sync.Mutex
	reads   map[string]int

	// ingestMu 是“写入生命周期门闩”：整个 Put（从开始暂存到元数据提交
	// 或回滚）持有读锁，GC 持有写锁。这样 GC 运行期间不存在
	// “blob 已发布但元数据尚未引用”的悬空窗口，杜绝误删新发布 blob。
	ingestMu sync.RWMutex

	// logf 用于打印内容摘要与引用计数变化（可解释性/测试日志）。
	logf func(format string, args ...any)

	// crashHook 仅供测试：在写入流程的指定阶段注入“进程退出”。
	// 取值：
	//   "afterIngest"  内容已流入暂存文件，但尚未校验发布（tmp 残留）；
	//   "afterPublish" blob 已原子发布，但元数据尚未提交（孤儿 blob）。
	crashHook func(stage string)
}

// Open 打开（或初始化）位于 dir 的存储，并执行中断恢复：
// 清理上次进程退出时遗留的暂存文件。元数据与 blob 均为原子发布，
// 故未完成写入不会留下任何“看似有效”的版本。
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("castore: open root: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, blobsDirName), 0o755); err != nil {
		return nil, fmt.Errorf("castore: ensure blobs dir: %w", err)
	}
	meta, err := loadMeta(dir)
	if err != nil {
		return nil, err
	}
	// 中断恢复：暂存区中的文件从未被元数据引用，直接清理。
	if err := cleanupTempBlobs(dir); err != nil {
		return nil, err
	}
	return &Store{
		root:  dir,
		meta:  meta,
		reads: map[string]int{},
		logf:  log.Printf,
	}, nil
}

// Close 关闭存储。
func (s *Store) Close() error { return nil }

// SetLogger 替换内部日志函数（打印摘要与引用计数变化）；传 nil 可静音。
func (s *Store) SetLogger(f func(format string, args ...any)) {
	if f == nil {
		s.logf = func(string, ...any) {}
		return
	}
	s.logf = f
}

// setCrashHook 仅供本包测试注入崩溃点；传 nil 清除。
func (s *Store) setCrashHook(f func(stage string)) { s.crashHook = f }

// Put 写入一份对象内容，返回新版本记录。
// 相同内容（SHA-256 相同）共享同一个 blob，重复上传不产生冗余副本；
// 并发写入相同内容时，blob 发布经原子 rename 收敛为一份，
// 版本各自登记但引用同一摘要，各次调用均得到相同 hash 与正确数据。
func (s *Store) Put(name string, data []byte) (Version, error) {
	return s.PutReader(name, bytes.NewReader(data), int64(len(data)))
}

// PutReader 是 Put 的流式形式，从 r 拷贝 size 字节。
// 崩溃可能发生在“流式写入暂存区之后”或“blob 发布之后、元数据提交之前”，
// 两种情况下重启都不会出现有效版本（详见 Open 的恢复逻辑）。
func (s *Store) PutReader(name string, r io.Reader, size int64) (Version, error) {
	if name == "" {
		return Version{}, fmt.Errorf("castore: object name must not be empty")
	}

	// 与 GC 互斥：在元数据提交完成前，本次写入发布的任何 blob
	// 都不得被 GC 当作零引用孤儿回收。
	s.ingestMu.RLock()
	defer s.ingestMu.RUnlock()

	// 1) 暂存 + 原子发布 blob（崩溃在这之前只留下 tmp 文件，Open 时清理）。
	bw, err := newBlobWriter(s.root)
	if err != nil {
		return Version{}, err
	}
	if _, err := io.Copy(bw, r); err != nil {
		bw.abort()
		return Version{}, fmt.Errorf("castore: write temp blob: %w", err)
	}
	if s.crashHook != nil {
		s.crashHook("afterIngest") // 测试：模拟写入过程中进程退出
	}
	hash, n, err := bw.commit(s.root)
	if err != nil {
		bw.abort()
		return Version{}, err
	}
	if s.crashHook != nil {
		s.crashHook("afterPublish") // 测试：blob 已在盘上，但元数据未提交
	}

	// 2) 串行提交元数据：登记版本 + 引用计数 +1。
	s.mu.Lock()
	nv := s.meta.NextVersion[name] // 不存在时为零值 0
	ver := Version{Object: name, Version: nv + 1, Hash: hash, Size: n}
	objs, ok := s.meta.Objects[name]
	if !ok {
		objs = map[uint64]Version{}
		s.meta.Objects[name] = objs
	}
	objs[ver.Version] = ver
	s.meta.NextVersion[name] = ver.Version
	before := s.meta.Refs[hash]
	s.meta.Refs[hash] = before + 1

	if err := s.meta.save(s.root); err != nil {
		// 回滚内存状态，使本实例与磁盘快照保持一致。
		delete(objs, ver.Version)
		if len(objs) == 0 {
			delete(s.meta.Objects, name)
		}
		if nv == 0 {
			delete(s.meta.NextVersion, name)
		} else {
			s.meta.NextVersion[name] = nv
		}
		if before == 0 {
			delete(s.meta.Refs, hash)
		} else {
			s.meta.Refs[hash] = before
		}
		s.mu.Unlock()
		// 已发布的 blob 此刻是磁盘上 refs=0 的孤儿，留待 GC 回收。
		return Version{}, err
	}
	s.mu.Unlock()

	s.logf("PUT   object=%q v=%d hash=%s size=%d refs[%s]: %d -> %d",
		name, ver.Version, hash, size, hash, before, before+1)
	return ver, nil
}

// Get 读取指定版本；version 传 0 表示读取最新存活版本。
// 数据返回前会重新计算 SHA-256 并与版本记录的寻址摘要比对。
func (s *Store) Get(name string, version uint64) ([]byte, Version, error) {
	s.mu.RLock()
	ver, err := s.lookupLocked(name, version)
	if err != nil {
		s.mu.RUnlock()
		return nil, Version{}, err
	}
	// 在读锁保护区内登记读取：与 GC 互斥，杜绝读到一半被回收。
	s.beginRead(ver.Hash)
	s.mu.RUnlock()
	defer s.endRead(ver.Hash)

	data, err := readBlob(s.root, ver.Hash)
	if err != nil {
		return nil, Version{}, err
	}
	s.logf("GET   object=%q v=%d hash=%s size=%d verified=true readers=%d",
		name, ver.Version, ver.Hash, ver.Size, s.readerCount(ver.Hash))
	return data, ver, nil
}

// Delete 删除某个版本；version 传 0 表示删除最新存活版本。
// 删除只改元数据（版本移除与引用计数 -1 在同一快照提交），
// blob 物理删除统一交给 GC，因此删除操作本身永远不会误删共享数据。
func (s *Store) Delete(name string, version uint64) (Version, error) {
	s.mu.Lock()
	ver, err := s.lookupLocked(name, version)
	if err != nil {
		s.mu.Unlock()
		return Version{}, err
	}
	before := s.meta.Refs[ver.Hash]

	delete(s.meta.Objects[name], ver.Version)
	if before <= 1 {
		delete(s.meta.Refs, ver.Hash)
	} else {
		s.meta.Refs[ver.Hash] = before - 1
	}

	if err := s.meta.save(s.root); err != nil {
		// 回滚内存状态。
		s.meta.Objects[name][ver.Version] = ver
		s.meta.Refs[ver.Hash] = before
		s.mu.Unlock()
		return Version{}, err
	}
	s.mu.Unlock()

	after := before - 1
	if before <= 1 {
		s.logf("DELETE object=%q v=%d hash=%s refs[%s]: %d -> 0 (eligible for GC)",
			name, ver.Version, ver.Hash, ver.Hash, before)
	} else {
		s.logf("DELETE object=%q v=%d hash=%s refs[%s]: %d -> %d (still referenced)",
			name, ver.Version, ver.Hash, ver.Hash, before, after)
	}
	return ver, nil
}

// Versions 返回对象的所有存活版本（按版本号升序）。
// 对象从未存在返回 ErrNotFound；所有版本均已删除时返回空切片。
func (s *Store) Versions(name string) ([]Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	objs, ok := s.meta.Objects[name]
	if !ok {
		if _, known := s.meta.NextVersion[name]; !known {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		return []Version{}, nil
	}
	out := make([]Version, 0, len(objs))
	for _, v := range objs {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// GC 回收“引用计数为 0 且无人读取”的 blob，
// 返回回收的摘要列表与释放的字节总数。
// 只以磁盘上实际存在的 blob 为候选，元数据中仍被引用或
// reads 计数非零的 blob 一律保留。
func (s *Store) GC() ([]string, int64, error) {
	// 等待所有进行中的写入完成元数据提交，避免误删“刚发布未引用”的 blob。
	s.ingestMu.Lock()
	defer s.ingestMu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	blobsRoot := filepath.Join(s.root, blobsDirName)
	var removed []string
	var freed int64

	err := filepath.WalkDir(blobsRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		hash := d.Name()
		if len(hash) != 64 { // 只处理按摘要命名的 blob
			return nil
		}
		refs := s.meta.Refs[hash]
		readers := s.readerCount(hash)
		if refs > 0 || readers > 0 {
			s.logf("GC    keep   hash=%s refs=%d readers=%d", hash, refs, readers)
			return nil
		}
		size, statErr := blobSize(s.root, hash)
		if statErr != nil {
			return statErr
		}
		if err := removeBlob(s.root, hash); err != nil {
			return err
		}
		removed = append(removed, hash)
		freed += size
		s.logf("GC    remove hash=%s size=%d refs=0 readers=0", hash, size)
		return nil
	})
	if err != nil {
		return removed, freed, fmt.Errorf("castore: gc: %w", err)
	}
	_ = fsyncDir(blobsRoot)
	s.logf("GC    done removed=%d bytesFreed=%d", len(removed), freed)
	return removed, freed, nil
}

// RefCount 返回某 blob 的当前引用计数（可解释性/测试辅助）。
func (s *Store) RefCount(hash string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.meta.Refs[hash]
}

// lookupLocked 在已持锁的前提下定位版本；version 为 0 取最新存活版本。
func (s *Store) lookupLocked(name string, version uint64) (Version, error) {
	objs, ok := s.meta.Objects[name]
	if !ok || len(objs) == 0 {
		return Version{}, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if version == 0 {
		var max uint64
		for vno := range objs {
			if vno > max {
				max = vno
			}
		}
		version = max
	}
	ver, ok := objs[version]
	if !ok {
		return Version{}, fmt.Errorf("%w: %s v%d", ErrNotFound, name, version)
	}
	return ver, nil
}

// beginRead/endRead 维护进行中的读取计数。
func (s *Store) beginRead(hash string) {
	s.readsMu.Lock()
	s.reads[hash]++
	s.readsMu.Unlock()
}

func (s *Store) endRead(hash string) {
	s.readsMu.Lock()
	n := s.reads[hash] - 1
	if n <= 0 {
		delete(s.reads, hash)
	} else {
		s.reads[hash] = n
	}
	s.readsMu.Unlock()
}

func (s *Store) readerCount(hash string) int {
	s.readsMu.Lock()
	defer s.readsMu.Unlock()
	return s.reads[hash]
}
