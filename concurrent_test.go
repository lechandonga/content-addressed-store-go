package cas

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentPutSameContent 验证：同一内容被大量 goroutine 并发写入不同 key、
// 同一 key 并发重复上传时，磁盘上最终只有一份数据块，所有写入的结论一致，
// 且引用计数恰好等于引用它的版本数。
func TestConcurrentPutSameContent(t *testing.T) {
	s := newTestStore(t)
	data := []byte("concurrent identical payload — every byte the same")
	d := digestData(data)

	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers*2)
	ids := make(chan int, workers*2)
	wg.Add(workers * 2)
	for i := 0; i < workers; i++ {
		// 不同 key，相同内容：应产生各自版本，但共享同一 blob。
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%02d", i)
			v, err := s.Put(key, data)
			if err != nil {
				errs <- err
				return
			}
			if v.Digest != d {
				errs <- fmt.Errorf("digest mismatch from worker %d", i)
				return
			}
			ids <- v.ID
		}(i)
		// 同一 key 并发重复上传：全部应落到同一个版本。
		go func() {
			defer wg.Done()
			v, err := s.Put("hot-key", data)
			if err != nil {
				errs <- err
				return
			}
			if v.Digest != d {
				errs <- fmt.Errorf("hot-key digest mismatch")
				return
			}
			ids <- v.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		t.Fatalf("concurrent put failed: %v", err)
	}

	hot, err := s.ListVersions("hot-key")
	if err != nil {
		t.Fatalf("ListVersions hot-key: %v", err)
	}
	if len(hot) != 1 {
		t.Fatalf("hot-key expected 1 version, got %d", len(hot))
	}

	if blobs := countBlobFiles(t, s.dir); blobs != 1 {
		t.Fatalf("expected exactly 1 blob after concurrent identical writes, got %d", blobs)
	}

	// 重新加载清单核对引用计数：32 个 key-x 版本 + 1 个 hot-key 版本 = 33。
	m, err := loadManifest(s.dir)
	if err != nil {
		t.Fatalf("loadManifest: %v", err)
	}
	if got := m.refCount(d); got != workers+1 {
		t.Fatalf("expected refs=%d for %s, got %d", workers+1, d, got)
	}
	t.Logf("concurrent dedup ok: digest=%s blobs-on-disk=1 refs=%d", d, workers+1)

	// 所有 key 读回内容必须逐字节正确。
	for i := 0; i < workers; i++ {
		key := fmt.Sprintf("key-%02d", i)
		got, _, err := s.Get(key)
		if err != nil || string(got) != string(data) {
			t.Fatalf("readback %s failed: %v", key, err)
		}
	}
}

// TestConcurrentPutDifferentContent 验证：并发写入不同内容时互不覆盖、各自可读。
func TestConcurrentPutDifferentContent(t *testing.T) {
	s := newTestStore(t)
	const n = 24
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			data := []byte(fmt.Sprintf("distinct payload number %04d with padding", i))
			key := fmt.Sprintf("obj-%02d", i)
			if _, err := s.Put(key, data); err != nil {
				t.Errorf("put %s: %v", key, err)
			}
		}()
	}
	wg.Wait()
	if blobs := countBlobFiles(t, s.dir); blobs != n {
		t.Fatalf("expected %d distinct blobs, got %d", n, blobs)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("distinct payload number %04d with padding", i)
		got, _, err := s.Get(fmt.Sprintf("obj-%02d", i))
		if err != nil || string(got) != want {
			t.Fatalf("obj-%02d readback: %v", i, err)
		}
	}
	t.Logf("distinct concurrent writes ok: blobs=%d", n)
}

// TestConcurrentCrossProcess 用两个独立的 Store 句柄模拟两个“进程”同时写同一内容。
// 它们共享磁盘目录，依靠 flock 串行化元数据提交，最终仍只保留一份数据块。
func TestConcurrentCrossProcess(t *testing.T) {
	dir := t.TempDir()
	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open s1: %v", err)
	}
	s1.WithLogger(t.Logf)
	t.Cleanup(func() { s1.Close() })
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open s2: %v", err)
	}
	s2.WithLogger(t.Logf)
	t.Cleanup(func() { s2.Close() })

	data := []byte("cross-process identical bytes")
	d := digestData(data)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		v, err := s1.Put("shared/key", data)
		if err == nil {
			t.Logf("writer1 got version v%d digest=%s", v.ID, v.Digest)
		}
	}()
	go func() {
		defer wg.Done()
		v, err := s2.Put("shared/key", data)
		if err == nil {
			t.Logf("writer2 got version v%d digest=%s", v.ID, v.Digest)
		}
	}()
	wg.Wait()

	if blobs := countBlobFiles(t, dir); blobs != 1 {
		t.Fatalf("cross-process dedup failed: blobs=%d", blobs)
	}
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.refCount(d); got != 1 {
		t.Fatalf("cross-process refs expected 1, got %d", got)
	}
	if versions := len(m.Keys["shared/key"]); versions != 1 {
		t.Fatalf("expected single version, got %d", versions)
	}
	got, _, err := s1.Get("shared/key")
	if err != nil || string(got) != string(data) {
		t.Fatalf("cross-process readback failed: %v", err)
	}
	t.Logf("cross-process concurrent dedup ok: digest=%s refs=1", d)
}
