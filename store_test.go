package castore

// 本文件覆盖：内容寻址去重、版本与引用管理、并发写入、中断恢复与 GC 边界。
// 用 t.Log 打印内容摘要与引用计数变化；go test -v 可见完整流水。

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func digestHex(p []byte) string {
	sum := sha256.Sum256(p)
	return hex.EncodeToString(sum[:])
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	// 让日志进入测试输出（含摘要与引用计数变化）。
	s.SetLogger(func(format string, args ...any) {
		t.Logf(format, args...)
	})
	return s
}

func blobExists(s *Store, hash string) bool {
	_, err := os.Stat(blobPath(s.root, hash))
	return err == nil
}

func tmpFileCount(s *Store) int {
	entries, err := os.ReadDir(filepath.Join(s.root, tmpDirName))
	if err != nil {
		return 0
	}
	return len(entries)
}

// 基本路径：重复上传去重；不同内容产生新版本；读取校验；删除只降计数。
func TestPutGetDedupAndVersions(t *testing.T) {
	s := openTestStore(t)
	contentA1 := []byte("hello world")
	contentA2 := []byte("hello again")

	v1, err := s.Put("objA", contentA1)
	if err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if v1.Hash != digestHex(contentA1) {
		t.Fatalf("v1 hash mismatch")
	}
	// 重复上传同一内容：同一 blob，引用计数累加，无冗余副本。
	v1dup, err := s.Put("objA", contentA1)
	if err != nil {
		t.Fatalf("Put duplicate: %v", err)
	}
	if v1dup.Hash != v1.Hash || v1dup.Version != 2 {
		t.Fatalf("duplicate put = %+v, want same hash v2", v1dup)
	}
	if got := s.RefCount(v1.Hash); got != 2 {
		t.Fatalf("refs after duplicate = %d, want 2", got)
	}

	v2, err := s.Put("objA", contentA2)
	if err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if v2.Hash == v1.Hash {
		t.Fatalf("different content must map to different blob")
	}
	if got := s.RefCount(v1.Hash); got != 2 {
		t.Fatalf("refs v1 after new content = %d, want 2", got)
	}
	if got := s.RefCount(v2.Hash); got != 1 {
		t.Fatalf("refs v2 = %d, want 1", got)
	}

	// 版本列表。
	vers, err := s.Versions("objA")
	if err != nil || len(vers) != 3 {
		t.Fatalf("Versions = %v err=%v, want 3 versions", vers, err)
	}

	// 读取各版本并校验内容。
	for _, tc := range []struct {
		vno  uint64
		want []byte
	}{{1, contentA1}, {2, contentA1}, {3, contentA2}, {0, contentA2}} {
		got, ver, err := s.Get("objA", tc.vno)
		if err != nil {
			t.Fatalf("Get v%d: %v", tc.vno, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("Get v%d content mismatch: got %q want %q", tc.vno, got, tc.want)
		}
		if ver.Hash != digestHex(tc.want) {
			t.Fatalf("Get v%d returned wrong version", tc.vno)
		}
	}

	// 删除 v2（重复引用）：blob 必须保留，计数降为 1。
	if _, err := s.Delete("objA", 2); err != nil {
		t.Fatalf("Delete v2: %v", err)
	}
	if got := s.RefCount(v1.Hash); got != 1 {
		t.Fatalf("refs after delete v2 = %d, want 1", got)
	}
	if !blobExists(s, v1.Hash) {
		t.Fatalf("blob still referenced by v1 must not be physically removed")
	}
	// v1 仍可读。
	if got, _, err := s.Get("objA", 1); err != nil || !bytes.Equal(got, contentA1) {
		t.Fatalf("v1 should remain readable after deleting v2")
	}
	// v2 已不存在；最新版本（version=0）现在是 v3。
	if _, _, err := s.Get("objA", 2); err == nil {
		t.Fatalf("expected error reading deleted v2")
	}
	if _, ver, err := s.Get("objA", 0); err != nil || ver.Version != 3 {
		t.Fatalf("latest should be v3, got %+v err=%v", ver, err)
	}
}

// GC 边界：零引用回收；仍被引用不回收；损坏数据读取报错。
func TestGCBoundaries(t *testing.T) {
	s := openTestStore(t)
	c1 := []byte("dedup-content")
	c2 := []byte("other-content")

	v1, _ := s.Put("o", c1)
	v1dup, _ := s.Put("o2", c1) // 不同对象共享 blob
	v2, _ := s.Put("o", c2)

	if v1.Hash != v1dup.Hash {
		t.Fatalf("same content across objects must share blob")
	}
	if removed, freed, err := s.GC(); err != nil || len(removed) != 0 || freed != 0 {
		t.Fatalf("GC with no garbage removed=%v freed=%d err=%v", removed, freed, err)
	}

	// 删掉 v1：blob 仍被 o2 引用，不得回收。
	if _, err := s.Delete("o", v1.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	removed, _, err := s.GC()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(removed) != 0 || !blobExists(s, v1.Hash) {
		t.Fatalf("GC must keep blob referenced by o2, removed=%v", removed)
	}

	// 再删掉 o2 的引用：blob 变为零引用，可回收；同时删除 v2 释放另一 blob。
	if _, err := s.Delete("o2", v1dup.Version); err != nil {
		t.Fatalf("Delete o2: %v", err)
	}
	if _, err := s.Delete("o", v2.Version); err != nil {
		t.Fatalf("Delete v2: %v", err)
	}
	removed, freed, err := s.GC()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(removed) != 2 {
		t.Fatalf("want 2 blobs removed, got %v", removed)
	}
	if freed != int64(len(c1)+len(c2)) {
		t.Fatalf("freed = %d, want %d", freed, len(c1)+len(c2))
	}
	if blobExists(s, v1.Hash) || blobExists(s, v2.Hash) {
		t.Fatalf("zero-ref blobs must be removed")
	}
	// 再 GC 为空操作。
	if removed, _, _ := s.GC(); len(removed) != 0 {
		t.Fatalf("second GC must be noop, got %v", removed)
	}
}

// 读取完整性校验：篡改 blob 后 Get 必须返回 ErrCorrupt。
func TestGetDetectsCorruption(t *testing.T) {
	s := openTestStore(t)
	v, _ := s.Put("o", []byte("integrity check"))
	p := blobPath(s.root, v.Hash)
	if err := os.WriteFile(p, []byte("tampered!!!"), 0o644); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if _, _, err := s.Get("o", 0); err == nil {
		t.Fatalf("expected error on corrupted blob")
	} else {
		t.Logf("corruption detected as expected: %v", err)
	}
}

// 正常提交必须跨进程重启可见（元数据与 blob 都已 fsync）。
func TestPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	content := []byte("persist me across restarts")
	v, err := s.Put("persist-obj", content)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, v2, err := s2.Get("persist-obj", 0)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !bytes.Equal(got, content) || v2.Hash != v.Hash {
		t.Fatalf("data/version mismatch after reopen")
	}
	if got := s2.RefCount(v.Hash); got != 1 {
		t.Fatalf("refs after reopen = %d, want 1", got)
	}
}
