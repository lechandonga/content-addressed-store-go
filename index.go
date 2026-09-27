package cas

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// 本文件负责元数据清单 meta.json 的读写与修复：
//   - keys 记录每个对象的版本链（版本 -> 数据块摘要）；
//   - refs 记录每个数据块的引用计数（被多少个版本引用）；
//   - 两者放在同一个清单文件中，一次原子 rename 提交，避免互相漂移。
//
// 崩溃一致性：
//   - 新数据块必须先 install 落盘并 fsync，之后才允许写入清单，
//     因此清单中引用的每个摘要一定指向已完整落盘的数据块；
//   - 清单用“临时文件 + fsync + rename + fsync 目录”提交，
//     崩溃只会发生在提交前或提交后，不会产生半个清单；
//   - 重启时 reconcileRefs 以版本链为准重算引用计数，消除极端断电
//     场景下 refs 与 keys 的任何偏差。
//

// metaVersion 是清单格式的版本号。
const metaVersion = 1

// storedVersion 是 Version 的持久化表示（时间用 UnixNano 避免时区歧义）。
type storedVersion struct {
	ID        int    `json:"id"`
	Digest    Digest `json:"digest"`
	Size      int64  `json:"size"`
	CreatedAt int64  `json:"created_at_unix_nano"`
}

func (sv storedVersion) toVersion(key string) Version {
	return Version{
		ID:        sv.ID,
		Key:       key,
		Digest:    sv.Digest,
		Size:      sv.Size,
		CreatedAt: time.Unix(0, sv.CreatedAt).UTC(),
	}
}

// manifest 是 meta.json 的完整内容。
type manifest struct {
	Version int                        `json:"version"`
	NextID  int                        `json:"next_id"`
	Keys    map[string][]storedVersion `json:"keys"`
	Refs    map[string]int             `json:"refs"`
}

// newManifest 返回一个空的、初始化好的清单。
func newManifest() *manifest {
	return &manifest{
		Version: metaVersion,
		NextID:  1,
		Keys:    map[string][]storedVersion{},
		Refs:    map[string]int{},
	}
}

// metaPath 返回清单文件路径。
func metaPath(dir string) string {
	return filepath.Join(dir, "meta", "meta.json")
}

// loadManifest 读取并解析清单；文件不存在时返回空清单。
func loadManifest(dir string) (*manifest, error) {
	data, err := os.ReadFile(metaPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return newManifest(), nil
		}
		return nil, fmt.Errorf("cas: read manifest: %w", err)
	}
	m := &manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("cas: parse manifest (storage corrupted): %w", err)
	}
	if m.Version != metaVersion {
		return nil, fmt.Errorf("cas: unsupported manifest version %d", m.Version)
	}
	if m.Keys == nil {
		m.Keys = map[string][]storedVersion{}
	}
	if m.Refs == nil {
		m.Refs = map[string]int{}
	}
	if m.NextID < 1 {
		m.NextID = 1
	}
	return m, nil
}

// saveManifest 以“临时文件 + fsync + 原子 rename”的方式持久化清单。
func saveManifest(dir string, m *manifest) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("cas: encode manifest: %w", err)
	}
	metaDir := filepath.Join(dir, "meta")
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return fmt.Errorf("cas: create meta dir: %w", err)
	}
	tmp, err := writeTemp(dir, buf.Bytes())
	if err != nil {
		return err
	}
	target := metaPath(dir)
	if err := os.Rename(tmp, target); err != nil {
		// rename 失败时尽量回收临时文件（即便失败，Open/GC 也会清扫 tmp/）。
		os.Remove(tmp)
		return fmt.Errorf("cas: install manifest rename: %w", err)
	}
	if err := fsyncDir(metaDir); err != nil {
		return err
	}
	return nil
}

// reconcileRefs 依据版本链重新计算引用计数，修复任何偏差。
// 返回内容为“摘要 -> 修正后的计数”，仅包含与旧值不同的条目；
// 计数为 0 的摘要仍保留在 map 中，由调用方据此执行删除。
func (m *manifest) reconcileRefs() map[string]int {
	want := map[string]int{}
	for _, versions := range m.Keys {
		for _, v := range versions {
			want[string(v.Digest)]++
		}
	}
	// 旧计数中存在、但已无版本引用的摘要，标记为 0（等待 GC）。
	changed := map[string]int{}
	for d, old := range m.Refs {
		if got := want[d]; got != old {
			changed[d] = got
		}
	}
	for d, got := range want {
		if old, ok := m.Refs[d]; !ok || old != got {
			changed[d] = got
		}
	}
	keys := make([]string, 0, len(want))
	for d := range want {
		keys = append(keys, d)
	}
	sort.Strings(keys)
	m.Refs = want
	return changed
}

// refCount 返回摘要当前的引用计数（不存在即 0）。
func (m *manifest) refCount(d Digest) int {
	return m.Refs[string(d)]
}
