package castore

// 本文件负责元数据的持久化：对象版本表与 blob 引用计数。
// 元数据通过 "写临时文件 + fsync + 原子 rename" 整体替换，
// 因此任意时刻磁盘上的 meta.json 要么是上一个完整版本，
// 要么是下一个完整版本，不会出现撕裂的元数据。
//
// 引用计数 Refs[hash] 等于所有对象所有存活版本对该 blob 的引用总数；
// 版本删除与计数递减在同一个元数据快照里提交，二者永不分离。

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// metaData 是元数据文件的完整快照。
type metaData struct {
	// Objects: 对象名 -> 版本号（从 1 递增）-> 版本记录。
	Objects map[string]map[uint64]Version `json:"objects"`
	// Refs: blob 摘要 -> 引用计数（所有未删除版本的引用总数）。
	Refs map[string]uint64 `json:"refs"`
	// NextVersion: 对象名 -> 下一个待分配的版本号。
	NextVersion map[string]uint64 `json:"next_version"`
}

// newMetaData 返回一个初始化好的空元数据快照。
func newMetaData() *metaData {
	return &metaData{
		Objects:     map[string]map[uint64]Version{},
		Refs:        map[string]uint64{},
		NextVersion: map[string]uint64{},
	}
}

// loadMeta 读取并解析元数据文件；文件不存在时返回空元数据。
// 解析失败视为元数据损坏并返回 ErrCorrupt，而不是静默清空
// （避免用空元数据启动后 GC 误删仍被引用的 blob）。
func loadMeta(dir string) (*metaData, error) {
	data, err := os.ReadFile(filepath.Join(dir, metaFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return newMetaData(), nil
		}
		return nil, fmt.Errorf("castore: read meta: %w", err)
	}
	var m metaData
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: meta.json: %v", ErrCorrupt, err)
	}
	if m.Objects == nil {
		m.Objects = map[string]map[uint64]Version{}
	}
	if m.Refs == nil {
		m.Refs = map[string]uint64{}
	}
	if m.NextVersion == nil {
		m.NextVersion = map[string]uint64{}
	}
	return &m, nil
}

// save 以原子替换方式持久化元数据。
// 临时文件与目标位于同一目录（同一文件系统），rename 才是原子的。
func (m *metaData) save(dir string) error {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("castore: encode meta: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".meta-*")
	if err != nil {
		return fmt.Errorf("castore: create meta temp: %w", err)
	}
	tmpName := tmp.Name()
	abort := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(buf); err != nil {
		abort()
		return fmt.Errorf("castore: write meta temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		abort()
		return fmt.Errorf("castore: sync meta temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		abort()
		return fmt.Errorf("castore: close meta temp: %w", err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, metaFileName)); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("castore: publish meta: %w", err)
	}
	// 确保 rename 本身落盘。
	if err := fsyncDir(dir); err != nil {
		return err
	}
	return nil
}
