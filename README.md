# 本地内容寻址对象存储（Go）

一个基于本地磁盘模拟后端的内容寻址对象存储（Content-Addressed Store, CAS）。
相同内容只落盘一份数据，对象由多个不可变版本组成，删除版本不会立即删除数据，
而是由垃圾回收在“没有任何版本引用、也没有任何人正在读取”时统一回收。

- 语言：Go（标准库实现，无第三方依赖）
- 寻址方式：SHA-256 内容摘要
- 并发协调：进程内 `sync.RWMutex` + 跨进程 `flock(2)` 文件锁

## 目录布局

```
<store-dir>/
├── lock                     # flock 锁文件（读者共享 / 写者与 GC 排他）
├── blobs/
│   └── <摘要前2字节>/
│       └── <完整 SHA-256 摘要>   # 不可变数据块，天然去重
├── tmp/
│   └── w-<随机>.tmp             # 写入中的临时文件，重启时整体清扫
└── meta/
    └── meta.json                # 清单：版本链 + 引用计数（单文件原子提交）
```

## 去重与版本模型

- **内容寻址与去重**：写入内容的 SHA-256 摘要就是它的唯一地址
  `blobs/<前2字节>/<完整摘要>`。相同内容无论上传多少次、写到多少个 key，
  磁盘上始终只有一份数据块。
- **版本是引用，不是数据副本**：每个对象（key）维护一条按版本号递增的不可变版本链，
  每个版本记录 `{id, digest, size, created_at}`，只“指向”一个数据块。
- **同一 key 重复上传相同内容**：直接返回既有版本，不新增版本、不重复计数，
  各次写入的结论（版本号、摘要）完全一致。
- **不同 key 写入相同内容**：各自产生版本，但共享同一数据块；数据块的引用计数
  等于引用它的版本总数。
- **读取完整性校验**：读取时重新计算 SHA-256，与版本记录的摘要逐位比对，
  不一致即返回 `ErrChecksum`，校验通过后才把字节交给调用方。

## 回收规则

- 删除版本（`DeleteVersion`）只解除引用：引用计数减一，数据块保留。
- `GC` 扫描所有数据块，**只删除引用计数为 0 的数据块**；仍被任意版本引用的
  数据块一律保留（`[gc] retain` 日志）。
- **正在被读取的数据块不会被删除**：读取持有 `flock` 共享锁，GC 需要排他锁，
  会阻塞到所有读者结束。进程内的读者由 `sync.RWMutex` 的读锁提供同样保证。
- 安装了数据块但清单从未提交的“孤儿数据块”（见下）在重启后不被任何版本引用，
  会被 `GC` 安全回收。

## 并发与中断恢复

写入严格按以下顺序持久化，因此任何一步崩溃都不会产生“半个有效版本”：

1. 内容写入 `tmp/` 临时文件并 `fsync`（无锁，并发写相同内容互不干扰）；
2. 取排他锁，把临时文件原子 `rename` 为 `blobs/<digest>`（目标已存在则丢弃临时文件，
   保证并发写同一内容只有一个胜出者）；
3. 最后才把新版本与引用计数写入 `meta.json`，同样以“临时文件 + fsync + rename”原子提交。

中断恢复发生在每次 `Open` 时（持排他锁）：

- 清空 `tmp/`：其中只可能是没走到提交步骤的写入残留，绝不会对应有效版本；
- 以版本链为准**重算引用计数**并修正 `meta.json`，消除极端断电可能留下的任何偏差
   （`[recover] refcount fixed` 日志）。

因此：崩溃前未提交的写入重启后不可见、无数据块（或仅为待 GC 的孤儿块）；
同一内容被并发写入时，最终只有一份数据块，所有写入者得到一致的结论。

## 可解释性日志

通过 `store.WithLogger(fn)` 注入日志回调（测试中接入 `t.Logf`），关键事件包括：

- `[put]`：写入的内容摘要、是否实际安装数据块、版本号、引用计数变化（`refs 1 -> 2`）；
- `[delete]`：删除版本后引用计数变化，以及数据块是否仍被保留；
- `[get]`：读取校验结果、摘要与当前引用计数；校验失败打印期望/实际摘要；
- `[gc] retain|collect|done`：每个数据块保留或回收的原因、字节数；
- `[recover]`：重启时清扫的临时文件与引用计数修正。

## API 速览

```go
s, _ := cas.Open("./data")
defer s.Close()
s.WithLogger(func(format string, args ...any) { log.Printf(format, args...) })

v1, _ := s.Put("reports/2026", []byte("..."))   // 写入新版本
data, v, err := s.Get("reports/2026")            // 读最新版本（校验摘要）
old, _, err := s.GetVersion("reports/2026", v1.ID) // 读指定版本
versions, _ := s.ListVersions("reports/2026")    // 列出全部版本
_ = s.DeleteVersion("reports/2026", v1.ID)       // 删除版本（仅解除引用）
report, _ := s.GC()                              // 回收零引用数据块 + 清扫临时文件
```

哨兵错误：`cas.ErrNotFound`（对象/版本不存在）、`cas.ErrChecksum`（数据块内容与摘要不符）。

## 本地验证

```bash
# 全部单元测试（含并发写入、中断恢复、回收边界）
go test -v -count=1 ./...

# 竞态检测（需要 cgo）
CGO_ENABLED=1 go test -race -count=1 ./...

# 静态检查
go vet ./...
```

测试覆盖：

| 测试 | 验证内容 |
| --- | --- |
| `TestPutGetAndChecksum` | 写入/读回一致、摘要正确 |
| `TestDeduplicationSingleBlob` | 同 key 重复上传与跨 key 相同内容，磁盘只有一份数据块 |
| `TestVersionsAndDeleteKeepsSharedBlob` | 版本链、共享数据块在仍被引用时不被 GC 回收 |
| `TestChecksumMismatchDetected` | 篡改数据块后读取返回 `ErrChecksum` |
| `TestConcurrentPutSameContent` | 64 个 goroutine 并发写相同内容：一份数据块、结论一致、引用计数精确 |
| `TestConcurrentPutDifferentContent` | 并发写不同内容互不覆盖 |
| `TestConcurrentCrossProcess` | 两个 Store 句柄（模拟两个进程）并发写相同内容，`flock` 保证唯一数据块 |
| `TestGCCannotDeleteBlobBeingRead` | 读取进行中 GC 必须等待，数据块不被删除 |
| `TestCrashDuringWrite` | 临时文件已写、清单未提交时“崩溃”，重启后无版本、无数据块、临时文件被清扫 |
| `TestCrashAfterBlobBeforeManifest` | 数据块已安装、清单未提交的孤儿块在重启后被 GC 回收 |
| `TestReopenRetainsVersionsAndRefs` | 正常重启后版本链、共享引用、可读性保持不变 |
