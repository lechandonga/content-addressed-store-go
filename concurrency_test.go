package castore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// 并发写入相同内容：最终只能保留一份 blob，所有调用得到相同 hash，
// 引用计数等于成功次数，每个版本都能读到正确原始字节。
func TestConcurrentPutSameContent(t *testing.T) {
	s := openTestStore(t)
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 251)
	}
	wantHash := digestHex(content)

	const n = 24
	var wg sync.WaitGroup
	errs := make(chan error, n)
	hashes := make(chan string, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			v, err := s.Put("hot", content)
			if err != nil {
				errs <- err
				return
			}
			hashes <- v.Hash
		}()
	}
	wg.Wait()
	close(errs)
	close(hashes)
	for err := range errs {
		t.Fatalf("concurrent Put: %v", err)
	}
	for h := range hashes {
		if h != wantHash {
			t.Fatalf("concurrent Put returned hash %s, want %s", h, wantHash)
		}
	}

	// 物理 blob 恰好一份：blobs 目录下只有一个文件。
	count := countFiles(t, filepath.Join(s.root, blobsDirName))
	if count != 1 {
		t.Fatalf("expected exactly 1 blob on disk, got %d", count)
	}
	// 引用计数 == 版本数 == n。
	if got := s.RefCount(wantHash); got != n {
		t.Fatalf("refs = %d, want %d", got, n)
	}
	vers, err := s.Versions("hot")
	if err != nil || len(vers) != n {
		t.Fatalf("Versions len=%d err=%v, want %d", len(vers), err, n)
	}
	// 每个版本读回内容一致。
	for _, v := range vers {
		got, _, gerr := s.Get("hot", v.Version)
		if gerr != nil || len(got) != len(content) || digestHex(got) != wantHash {
			t.Fatalf("version %d unreadable or wrong: err=%v", v.Version, gerr)
		}
	}
}

// 并发混合负载：不同对象/不同内容/重复内容 + 并发删除与 GC，
// 在 -race 下反复运行验证无数据竞争且结论一致。
func TestConcurrentMixedWorkload(t *testing.T) {
	s := openTestStore(t)
	contents := [][]byte{[]byte("alpha"), []byte("beta"), []byte("alpha"), []byte("gamma")}

	const goroutines = 16
	const rounds = 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				name := fmt.Sprintf("obj-%d", g%4)
				data := contents[r%len(contents)]
				v, err := s.Put(name, data)
				if err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				got, _, err := s.Get(name, v.Version)
				if err != nil || digestHex(got) != v.Hash {
					t.Errorf("Get after Put mismatch: %v", err)
					return
				}
				if r%5 == 0 {
					_, _, _ = s.GC()
				}
			}
		}(g)
	}
	wg.Wait()

	// 最终不变量：每个存活版本指向的 blob 必须存在且内容校验通过；
	// 引用计数之和等于存活版本数。
	s.mu.RLock()
	var totalRefs uint64
	type ref struct {
		name string
		vno  uint64
		hash string
	}
	var live []ref
	for name, versions := range s.meta.Objects {
		for vno, v := range versions {
			live = append(live, ref{name, vno, v.Hash})
		}
	}
	for _, n := range s.meta.Refs {
		totalRefs += n
	}
	s.mu.RUnlock()
	if uint64(len(live)) != totalRefs {
		t.Fatalf("live versions=%d but total refs=%d", len(live), totalRefs)
	}
	for _, r := range live {
		if !blobExists(s, r.hash) {
			t.Fatalf("blob %s missing while referenced by %s v%d", r.hash, r.name, r.vno)
		}
		if data, _, err := s.Get(r.name, r.vno); err != nil || digestHex(data) != r.hash {
			t.Fatalf("live version %s v%d not verifiable: %v", r.name, r.vno, err)
		}
	}
}

// GC 不得回收正在被读取的 blob：手工持有一个进行中的读取，
// 此时 GC 必须保留该 blob；读取结束后 GC 才能回收。
func TestGCSkipsInFlightRead(t *testing.T) {
	s := openTestStore(t)
	v, _ := s.Put("o", []byte("read while gc"))
	if _, err := s.Delete("o", v.Version); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := s.RefCount(v.Hash); got != 0 {
		t.Fatalf("refs after delete = %d, want 0", got)
	}

	// 模拟一个读者已经打开 blob、正在读取（与 Get 内部同一套保护）。
	s.beginRead(v.Hash)
	removed, _, err := s.GC()
	if err != nil {
		t.Fatalf("GC during read: %v", err)
	}
	if len(removed) != 0 || !blobExists(s, v.Hash) {
		t.Fatalf("GC must not remove a blob being read, removed=%v", removed)
	}
	s.endRead(v.Hash)

	removed, _, err = s.GC()
	if err != nil || len(removed) != 1 || blobExists(s, v.Hash) {
		t.Fatalf("GC after read should remove blob, removed=%v err=%v", removed, err)
	}
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			n++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk: %v", err)
	}
	return n
}
