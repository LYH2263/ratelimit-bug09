package ratelimit

import (
	"context"
	"testing"
	"time"
)

type seqCancel struct {
	left int
}

func (s *seqCancel) Deadline() (time.Time, bool) { return time.Time{}, false }
func (s *seqCancel) Done() <-chan struct{}       { return nil }
func (s *seqCancel) Value(any) any               { return nil }
func (s *seqCancel) Err() error {
	if s.left > 0 {
		s.left--
		return nil
	}
	return context.Canceled
}

func TestBug09_AllowCtxCancelRollback(t *testing.T) {
	l, _ := New(Options{})
	defer l.Close()
	_ = l.SetQuota(context.Background(), "k", Quota{Rate: 1, Burst: 1})
	// 前两次 Err() 放行，AllowN 扣令牌后第三次 Err() 取消，须 credit 回滚。
	ctx := &seqCancel{left: 2}
	ok, err := l.AllowCtx(ctx, "k")
	if ok || err == nil {
		t.Fatal("expected cancel failure after take")
	}
	if !l.Allow("k") {
		t.Fatal("token not rolled back after canceled AllowCtx")
	}
}
