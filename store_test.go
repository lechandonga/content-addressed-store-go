package cas

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// newTestStore 打开一个位于临时目录、带 t.Logf 日志的存储。
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	s.WithLogger(t.Logf)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetAndChecksum(t *testing.T) {
	s := newTestStore(t)
	data := []byte("hello content-addressed world")
	v1, err := s.Put("obj/a", data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if v1.Digest != digestData(data) {
		t.Fatalf("digest mismatch")
	}
	got, v2, err := s.Get("obj/a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("content mismatch: %q", got)
	}
	if v1.ID != v2.ID {
		t.Fatalf("version mismatch: %d vs %d", v1.ID, v2.ID)
	}
}

func TestDeduplicationSingleBlob(t *testing.T) {
	s := newTestStore(t)
	data := []byte("the same payload bytes")

	v1, err := s.Put("k1", data)
	if err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	// 同一 key 重复上传：返回既有版本，不产生新版本。
	v1dup, err := s.Put("k1", data)
	if err != nil {
		t.Fatalf("Put k1 dup: %v", err)
	}
	if v1dup.ID != v1.ID {
		t.Fatalf("duplicate upload created new version: %d vs %d", v1dup.ID, v1.ID)
	}
	// 不同 key、相同内容：引用同一个数据块。
	v2, err := s.Put("k2", data)
	if err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	if v2.Digest != v1.Digest {
		t.Fatalf("digests should be equal")
	}

	blobs := countBlobFiles(t, s.dir)
	if blobs != 1 {
		t.Fatalf("expected exactly 1 blob on disk after dedup, got %d", blobs)
	}
	t.Logf("dedup ok: digest=%s, blobs-on-disk=%d", v1.Digest, blobs)
}

func TestVersionsAndDeleteKeepsSharedBlob(t *testing.T) {
	s := newTestStore(t)
	shared := []byte("shared content between versions and keys")
	other := []byte("a distinct payload")

	va, _ := s.Put("doc", shared)
	vb, _ := s.Put("doc", other)        // doc: v1(shared), v2(other)
	vc, _ := s.Put("other-doc", shared) // other-doc: v1(shared)

	versions, err := s.ListVersions("doc")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(versions) != 2 || versions[0].ID != va.ID || versions[1].ID != vb.ID {
		t.Fatalf("unexpected versions: %+v", versions)
	}

	// 删除 doc 的 v1(shared)，但 other-doc 仍引用它，GC 不得回收。
	if err := s.DeleteVersion("doc", va.ID); err != nil {
		t.Fatalf("DeleteVersion: %v", err)
	}
	rep, err := s.GC()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(rep.Removed) != 0 {
		t.Fatalf("GC removed still-referenced blob %v", rep.Removed)
	}
	if _, _, err := s.GetVersion("other-doc", vc.ID); err != nil {
		t.Fatalf("shared blob must remain readable: %v", err)
	}
	t.Logf("retained shared digest=%s as expected", va.Digest)

	// 再删除 other-doc 的引用 -> shared 无引用，GC 应回收；
	// doc v2(other) 仍被引用，必须保留。
	if err := s.DeleteVersion("other-doc", vc.ID); err != nil {
		t.Fatalf("DeleteVersion other-doc: %v", err)
	}
	rep, err = s.GC()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(rep.Removed) != 1 || rep.Removed[0] != va.Digest {
		t.Fatalf("expected exactly %s collected, got %v", va.Digest, rep.Removed)
	}
	blobs := countBlobFiles(t, s.dir)
	if blobs != 1 {
		t.Fatalf("expected 1 surviving blob (other), got %d", blobs)
	}
	if _, _, err := s.Get("doc"); err != nil {
		t.Fatalf("doc latest version must remain: %v", err)
	}
}

func TestChecksumMismatchDetected(t *testing.T) {
	s := newTestStore(t)
	data := []byte("integrity check payload")
	v, _ := s.Put("key", data)

	// 人为篡改磁盘上的数据块。
	p := blobPath(s.dir, v.Digest)
	if err := os.WriteFile(p, []byte("TAMPERED"), 0o600); err != nil {
		t.Fatalf("tamper blob: %v", err)
	}
	if _, _, err := s.Get("key"); err == nil {
		t.Fatalf("expected checksum error after tampering")
	} else if !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got %v", err)
	}
}

func countBlobFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(filepath.Join(dir, "blobs"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk blobs: %v", err)
	}
	return n
}
