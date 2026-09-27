package cas

import (
	"fmt"
	"os"
	"syscall"
)

// fileLock 封装基于 flock(2) 的文件锁，用于跨进程互斥：
//   - 共享锁（LOCK_SH）：读取数据块期间持有，与其他读者兼容；
//   - 排他锁（LOCK_EX）：元数据变更与 GC 期间持有，与一切其它锁互斥。
//
// flock 同时也提供同进程多 goroutine 间的排队语义（阻塞调用串行化），
// 但为了读写锁的并发语义，同进程内仍由 Store.mu（sync.RWMutex）协调。
type fileLock struct {
	f      *os.File
	locked bool // 当前是否持有任意一种 flock（用于幂等释放）
}

// newFileLock 在 path 上创建/打开锁文件。
func newFileLock(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cas: open lock file %q: %w", path, err)
	}
	return &fileLock{f: f}, nil
}

// rlock 获取共享锁（阻塞直到可用）。
func (l *fileLock) rlock() error {
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("cas: acquire shared lock: %w", err)
	}
	l.locked = true
	return nil
}

// lock 获取排他锁（阻塞直到可用）。
func (l *fileLock) lock() error {
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("cas: acquire exclusive lock: %w", err)
	}
	l.locked = true
	return nil
}

// unlock 释放当前持有的锁（若未持有则为空操作）。
// 同一 fd 上把 LOCK_SH 升级/切换为 LOCK_EX 由调用方保证不会并发混用。
func (l *fileLock) unlock() error {
	if !l.locked {
		return nil
	}
	if err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("cas: release lock: %w", err)
	}
	l.locked = false
	return nil
}

// close 释放锁并关闭锁文件句柄。
func (l *fileLock) close() error {
	unlockErr := l.unlock()
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("cas: close lock file: %w", err)
	}
	return unlockErr
}
