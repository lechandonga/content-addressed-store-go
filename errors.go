package castore

// 本文件定义存储对外暴露的哨兵错误。

import "errors"

// ErrNotFound 表示对象或指定版本不存在。
var ErrNotFound = errors.New("castore: object or version not found")

// ErrCorrupt 表示数据内容与寻址摘要不一致。
var ErrCorrupt = errors.New("castore: content corrupted")
