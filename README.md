# content-addressed-store-go

一个用本地磁盘模拟后端的**内容寻址对象存储**，面向重复写入、版本删除与数据回收，
在**并发**与**中断**场景下都给出一致且可解释的结果。纯 Go 标准库实现，无外部依赖。

## 快速开始

```go
s, _ := castore.Open("./data")
defer s.Close()

v1, _ := s.Put("report.txt", []byte("hello"))      // 写入，返回版本记录
v2, _ := s.Put("report.txt", []byte("hello"))      // 相同内容：共享同一份 blob，refs +1
data, ver, err := s.Get("report.txt", 0)           // version=0 取最新版本，读取前校验 SHA-256
_, _ = s.Delete("report.txt", v1.Version)          // 只删版本引用，不立即删数据
removed, freed, _ := s.GC()                        // 真正回收 refs=0 且无人读取的 blob
_ = v2
```

## 磁盘布局

```
<root>/
├── meta.json          # 元数据快照（版本表 + 引用计数），整体原子替换
├── blobs/<前2位摘要>/<完整 SHA-256 摘要>   # 不可变数据块，按摘要分桶
└── tmp/ingest-*       # 写入暂存区，重启时整体清空
```

## 去重与版本模型

- **寻址键 = 内容 SHA-256**。对象数据先写入 `tmp/` 暂存文件（写入时同步计算摘要），
  `fsync` 后通过同目录原子 `rename` 发布为 `blobs/xx/<hash>`。
- **相同内容只有一份数据**。发布时若目标 blob 已存在则丢弃本次暂存（去重命中）；
  并发写入相同内容时，两个写者 rename 到同一路径，POSIX rename 的原子性保证
  最终磁盘上只有一份正确字节，而所有写入调用都得到**相同 hash、相同结论**。
- **对象 → 多个不可变版本**。每次 `Put` 都分配一个单调递增的新版本号；
  版本记录 `{object, version, hash, size}` 不可变，多个版本（甚至不同对象）
  可引用同一 blob。
- **引用计数即“被多少个存活版本引用”**。`Put` 时 `refs[hash]++`，
  `Delete` 时 `refs[hash]--`；版本登记与计数变更在**同一个元数据快照**里提交，
  二者不会分离。可通过 `RefCount(hash)` 查询，日志也会打印每次变化。

### 读取完整性

`Get` 读取 blob 时重新流式计算 SHA-256 并与寻址摘要比对，任何不一致都返回
`castore.ErrCorrupt`，损坏字节不会返回给调用方。

## 回收规则（GC）

`Delete` **从不物理删除数据**，只修改元数据。物理回收统一由 `GC()` 完成，
一个 blob 被回收当且仅当同时满足：

1. 磁盘上存在该 blob；
2. **引用计数为 0**（没有任何存活版本引用它）；
3. **没有正在进行中的读取**。

为此有两道保护：

- **读保护**：`Get` 在与元数据变更互斥的临界区内登记 `readers[hash]++`，
  随后释放元数据锁去读文件，读完再 `--`。GC 持写锁扫描时，
  任何可能进行中的读取都一定已登记，因此绝不会删掉正在被读的数据。
- **写入门闩**：从“开始暂存”到“元数据提交/回滚”的整个 `Put` 生命周期持有
  一把 RWMutex 的读锁，GC 取写锁。这消除了“blob 已发布但元数据尚未引用”
  的悬空窗口，避免 GC 误删刚发布的 blob。

写入失败或元数据提交失败时，内存状态随磁盘快照一并回滚；已经发布到磁盘的 blob
此时是 `refs=0` 的孤儿，安全地留给下一次 GC 回收。

## 并发与中断一致性

| 关注点 | 机制 |
| --- | --- |
| 元数据并发修改 | 单一读写锁串行化提交；每次提交写全量 JSON 快照 |
| 元数据崩溃撕裂 | 写临时文件 → `fsync` → 原子 `rename` → `fsync` 目录 |
| 写入中途进程退出 | 暂存文件从未发布；重启时 `tmp/` 整体清空，不产生任何版本 |
| blob 发布后、元数据提交前退出 | blob 是不可变且内容正确的，但无版本引用，成为 `refs=0` 孤儿，GC 回收；重启后重写相同内容会复用它 |
| 并发相同内容 | 原子 rename 收敛为一份 blob，引用计数正确累加 |

**关键顺序：先发布不可变 blob（fsync），再提交元数据。** 因此磁盘上永远不会出现
“元数据指向一个半写 blob”的状态；最坏情况只是留下一个无引用的完整孤儿。

## 本地验证

```bash
# 全量单测（包含并发与子进程崩溃恢复）
go test -v ./...

# 竞态检测（需要 cgo/gcc）
CGO_ENABLED=1 go test -race -count=3 ./...

go vet ./...
```

测试覆盖与对应场景：

| 测试 | 覆盖点 |
| --- | --- |
| `TestPutGetDedupAndVersions` | 重复上传共享 blob、版本递增、读取校验、删除后最新版本选择 |
| `TestGCBoundaries` | 仍被引用不回收、跨对象共享不回收、零引用回收、释放字节数 |
| `TestGCSkipsInFlightRead` | 正在读取的 blob 禁止回收，读完后才允许回收 |
| `TestConcurrentPutSameContent` | 24 个 goroutine 并发写相同内容：仅一份 blob、refs=24、各版本可读 |
| `TestConcurrentMixedWorkload` | 并发写/读/删除/GC 混合负载，校验存活版本与引用计数不变量（`-race`） |
| `TestCrashAfterIngest` | 子进程在暂存后、发布前 `os.Exit`：重启清 tmp，无版本无 blob |
| `TestCrashAfterPublish` | 子进程在发布后、元数据提交前 `os.Exit`：无版本，孤儿 blob 被 GC 回收 |
| `TestRecoverThenRewrite` | 崩溃恢复后重写相同内容成功且数据正确 |
| `TestPersistAcrossReopen` | 正常提交跨重启可见 |
| `TestGetDetectsCorruption` | 篡改 blob 后读取返回 `ErrCorrupt` |

用 `-v` 运行时，日志会打印每次操作的**内容摘要**与**引用计数变化**，例如：

```
PUT    object="o" v=1 hash=ab3349…253e size=13 refs[ab3349…253e]: 0 -> 1
DELETE object="o" v=1 hash=ab3349…253e refs[ab3349…253e]: 1 -> 0 (eligible for GC)
GC     keep   hash=ab3349…253e refs=1 readers=0
GC     remove hash=ab3349…253e size=13 refs=0 readers=0
```

## API

- `Open(dir string) (*Store, error)` — 打开/初始化并执行中断恢复
- `(*Store) Put(name string, data []byte) (Version, error)`
- `(*Store) PutReader(name string, r io.Reader, size int64) (Version, error)` — 流式写入
- `(*Store) Get(name string, version uint64) ([]byte, Version, error)` — `version=0` 为最新版本
- `(*Store) Delete(name string, version uint64) (Version, error)`
- `(*Store) Versions(name string) ([]Version, error)` — 存活版本，按版本号升序
- `(*Store) GC() (removed []string, bytesFreed int64, err error)`
- `(*Store) RefCount(hash string) uint64`
- `(*Store) SetLogger(func(format string, args ...any))` — 自定义摘要/引用计数日志
