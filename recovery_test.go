package castore

import (
	"encoding/hex"
	"os"
	"os/exec"
	"testing"
)

// recoverAndCheck 重新打开目录，断言：无 tmp 残留、无幽灵版本、
// 给定内容的 blob 存活状态符合 wantBlob。
func recoverAndCheck(t *testing.T, dir string, wantBlob bool, content []byte) *Store {
	t.Helper()
	// 重新打开即触发中断恢复（清理 tmp、加载最后完整元数据快照）。
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	s.SetLogger(func(format string, args ...any) { t.Logf(format, args...) })
	t.Cleanup(func() { _ = s.Close() })

	if n := countTmp(t, dir); n != 0 {
		t.Fatalf("after recovery: %d stale temp files remain", n)
	}
	if data, _, err := s.Get("o", 0); err == nil {
		t.Fatalf("interrupted write must not become a valid version, got data len=%d", len(data))
	}
	hash := digestHex(content)
	if got := blobExists(s, hash); got != wantBlob {
		t.Fatalf("blob presence after crash = %v, want %v", got, wantBlob)
	}
	t.Logf("recovery OK: no live version for interrupted put; orphan blob present=%v", blobExists(s, hash))
	return s
}

func namesOf(entries []os.DirEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func countTmp(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir + "/" + tmpDirName)
	if err != nil {
		return 0
	}
	return len(entries)
}

// 场景一：进程在“内容流入暂存文件、尚未发布 blob”时退出。
// 重启后 tmp 被清理，无版本、无 blob。
func TestCrashAfterIngest(t *testing.T) {
	dir := t.TempDir()
	content := []byte("crash after ingest")
	out, err := runCrashChild(t, dir, "afterIngest", content)
	if err != nil {
		t.Logf("child output:\n%s", out)
	}

	recoverAndCheck(t, dir, false, content)
}

// 场景二：进程在“blob 已发布、元数据尚未提交”时退出。
// 重启后无版本，但磁盘留有一个 refs=0 的孤儿 blob，GC 可回收。
func TestCrashAfterPublish(t *testing.T) {
	dir := t.TempDir()
	content := []byte("crash after publish")
	out, err := runCrashChild(t, dir, "afterPublish", content)
	if err != nil {
		t.Logf("child output:\n%s", out)
	}

	s := recoverAndCheck(t, dir, true, content)
	hash := digestHex(content)
	if got := s.RefCount(hash); got != 0 {
		t.Fatalf("orphan blob refs = %d, want 0", got)
	}
	removed, _, err := s.GC()
	if err != nil || len(removed) != 1 || removed[0] != hash {
		t.Fatalf("GC should reclaim orphan, removed=%v err=%v", removed, err)
	}
}

// 崩溃恢复后重新写入相同内容：应正常成功且数据正确。
func TestRecoverThenRewrite(t *testing.T) {
	dir := t.TempDir()
	content := []byte("rewrite after crash")
	if _, err := runCrashChild(t, dir, "afterPublish", content); err != nil {
		t.Logf("child exited as designed")
	}
	s := recoverAndCheck(t, dir, true, content)
	// 孤儿 blob 会被新写入复用（发布时发现目标已存在直接去重命中）。
	v, err := s.Put("o", content)
	if err != nil {
		t.Fatalf("Put after recovery: %v", err)
	}
	if v.Hash != digestHex(content) {
		t.Fatalf("hash mismatch after rewrite")
	}
	got, _, err := s.Get("o", v.Version)
	if err != nil || string(got) != string(content) {
		t.Fatalf("content after rewrite wrong: %v", err)
	}
}

const crashChildEnv = "CASTORE_CRASH_CHILD"

// runCrashChild 在子进程中执行一次会在指定阶段“崩溃”的 Put。
func runCrashChild(t *testing.T, dir, stage string, content []byte) ([]byte, error) {
	t.Helper()
	hexContent := hex.EncodeToString(content)
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashChildProcess", "-test.v")
	cmd.Env = append(os.Environ(),
		crashChildEnv+"=1",
		"CASTORE_DIR="+dir,
		"CASTORE_STAGE="+stage,
		"CASTORE_CONTENT="+hexContent,
	)
	return cmd.CombinedOutput()
}

// TestCrashChildProcess 只在子进程中实际运行：打开存储、注入退出点并 Put。
func TestCrashChildProcess(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		t.Skip("crash child helper only")
	}
	dir := os.Getenv("CASTORE_DIR")
	stage := os.Getenv("CASTORE_STAGE")
	raw, _ := hex.DecodeString(os.Getenv("CASTORE_CONTENT"))

	s, err := Open(dir)
	if err != nil {
		t.Fatalf("child Open: %v", err)
	}
	s.setCrashHook(func(got string) {
		if got == stage {
			t.Logf("child simulating process exit at stage=%s content-hash=%s",
				stage, digestHex(raw))
			_ = s.Close()
			os.Exit(17)
		}
	})
	v, err := s.Put("o", raw)
	if err != nil {
		// 正常情况下不会走到这里（hook 已退出）。
		t.Fatalf("child Put unexpectedly returned: %+v %v", v, err)
	}
	os.Exit(0)
}
