package cas

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestGCCannotDeleteBlobBeingRead 验证：读取窗口未结束时（持有共享锁），
// 即使版本已被删除、引用计数归零，GC 也必须阻塞等待，不能删掉正在读的数据。
func TestGCCannotDeleteBlobBeingRead(t *testing.T) {
	s := newTestStore(t)
	data := []byte("payload that is being streamed to a reader")
	v, err := s.Put("ephemeral", data)
	if err != nil {
		t.Fatal(err)
	}
	d := v.Digest

	// 先删除版本：此时 blob 逻辑上已无引用，但仍有读者即将开始。
	if err := s.DeleteVersion("ephemeral", v.ID); err != nil {
		t.Fatal(err)
	}

	gcDone := make(chan *GCReport, 1)
	readerEntered := make(chan struct{})
	releaseReader := make(chan struct{})

	// 模拟一个“读者已打开并校验数据、读取尚未完成”的过程：
	// 持有共享锁读取 blob 期间，GC 的排他锁必须等待。
	go func() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if err := s.flock.rlock(); err != nil {
			t.Errorf("reader rlock: %v", err)
			return
		}
		defer s.flock.unlock()

		blobData, _, err := readBlob(s.dir, d)
		if err != nil {
			t.Errorf("reader open blob: %v", err)
			return
		}
		if digestData(blobData) != d {
			t.Errorf("reader verified bad digest")
			return
		}
		close(readerEntered)
		<-releaseReader // 模拟读取耗时
	}()

	<-readerEntered
	t.Logf("reader is mid-flight on digest=%s; attempting GC (must wait)", d)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rep, err := s.GC()
		if err != nil {
			t.Errorf("GC: %v", err)
			return
		}
		gcDone <- rep
	}()

	// GC 必须被共享锁挡住；等待期间 blob 文件必须仍然存在。
	select {
	case <-gcDone:
		t.Fatalf("GC completed while a read was in progress")
	case <-time.After(100 * time.Millisecond):
		if _, err := os.Stat(blobPath(s.dir, d)); err != nil {
			t.Fatalf("blob removed while being read: %v", err)
		}
		t.Logf("GC correctly blocked; digest=%s still on disk during read", d)
	}

	close(releaseReader)
	wg.Wait()
	rep := <-gcDone
	if len(rep.Removed) != 1 || rep.Removed[0] != d {
		t.Fatalf("after reader finished, GC should collect %s, got %v", d, rep.Removed)
	}
}

// TestCrashDuringWrite 模拟写入过程中进程退出：
// 只留下 tmp/ 临时文件、清单从未提交。重启后不得出现任何有效版本，
// 临时文件被清扫，重复完整写入后一切正常。
func TestCrashDuringWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.WithLogger(t.Logf)

	data := []byte("payload whose commit never happened")
	d := digestData(data)

	// 模拟崩溃：临时文件已写完并 fsync，但尚未 install / 未更新清单。
	tmp, err := writeTemp(dir, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatal(err)
	}
	t.Logf("simulated crash after temp write: %s digest=%s", filepath.Base(tmp), d)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启。
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.WithLogger(t.Logf)
	t.Cleanup(func() { s2.Close() })

	if _, _, err := s2.Get("key-that-never-committed"); err == nil {
		t.Fatalf("uncommitted write must not be readable after restart")
	}
	if versions, _ := s2.ListVersions("key-that-never-committed"); versions != nil {
		t.Fatalf("uncommitted write must not appear in version list")
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "tmp")); len(entries) != 0 {
		t.Fatalf("leftover temp files not swept: %v", entries)
	}
	if blobs := countBlobFiles(t, dir); blobs != 0 {
		t.Fatalf("no blob should have been installed, found %d", blobs)
	}
	t.Logf("recovery ok: no version, no blob, tmp swept for digest=%s", d)

	// 重新完整写入相同内容，应作为首个正常版本成功。
	v, err := s2.Put("key-that-never-committed", data)
	if err != nil {
		t.Fatalf("rewrite after recovery: %v", err)
	}
	if v.ID != 1 {
		t.Fatalf("expected recovered store to start versioning at 1, got %d", v.ID)
	}
	got, _, err := s2.Get("key-that-never-committed")
	if err != nil || string(got) != string(data) {
		t.Fatalf("readback after recovery failed: %v", err)
	}
}

// TestCrashAfterBlobBeforeManifest 模拟数据块已安装、但清单提交前崩溃：
// 数据块成为孤儿，重启后不存在引用它的版本，GC 应将其安全回收。
func TestCrashAfterBlobBeforeManifest(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.WithLogger(t.Logf)

	data := []byte("orphaned blob bytes awaiting garbage collection")
	d := digestData(data)
	tmp, err := writeTemp(dir, data)
	if err != nil {
		t.Fatal(err)
	}
	installed, err := installBlob(dir, d, tmp)
	if err != nil || !installed {
		t.Fatalf("installBlob: installed=%v err=%v", installed, err)
	}
	// 不保存清单 => 模拟在清单提交前进程退出。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("simulated crash after blob install, before manifest commit: digest=%s", d)

	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s2.WithLogger(t.Logf)
	t.Cleanup(func() { s2.Close() })

	if _, err := os.Stat(blobPath(dir, d)); err != nil {
		t.Fatalf("orphan blob should survive restart until GC: %v", err)
	}
	rep, err := s2.GC()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if len(rep.Removed) != 1 || rep.Removed[0] != d {
		t.Fatalf("orphan blob %s should be collected, got %v", d, rep.Removed)
	}
	t.Logf("orphan digest=%s collected after recovery, bytes freed=%d", d, rep.BytesFreed)
}

// TestReopenRetainsVersionsAndRefs 验证正常关闭重开后版本链与读取均保持一致。
func TestReopenRetainsVersionsAndRefs(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.WithLogger(t.Logf)
	a := []byte("version alpha")
	b := []byte("version beta")
	v1, _ := s.Put("doc", a)
	v2, _ := s.Put("doc", b)
	_, _ = s.Put("mirror", a) // a 被两个版本引用
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2.WithLogger(t.Logf)
	t.Cleanup(func() { s2.Close() })

	versions, err := s2.ListVersions("doc")
	if err != nil || len(versions) != 2 || versions[0].ID != v1.ID || versions[1].ID != v2.ID {
		t.Fatalf("version chain lost across restart: %v %v", versions, err)
	}
	got, _, err := s2.GetVersion("doc", v1.ID)
	if err != nil || string(got) != string(a) {
		t.Fatalf("old version not readable after restart: %v", err)
	}
	rep, err := s2.GC()
	if err != nil || len(rep.Removed) != 0 {
		t.Fatalf("nothing should be collected after clean restart: %v %v", rep, err)
	}
	m, _ := loadManifest(dir)
	if n := m.refCount(digestData(a)); n != 2 {
		t.Fatalf("refs for shared blob expected 2, got %d", n)
	}
	t.Logf("reopen ok: %d versions, shared digest=%s refs=2", len(versions), digestData(a))
}
