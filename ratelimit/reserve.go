package ratelimit

import (
	"time"

	"github.com/LYH2263/go-ratelimit/internal/bucket"
)

type Reservation struct {
	ok      bool
	tokens  int
	delay   time.Duration
	limiter *Limiter
	key     string
	held    bool
}

func (r *Reservation) OK() bool             { return r.ok }
func (r *Reservation) Delay() time.Duration { return r.delay }

// Reserve 预约 n 个令牌。能立即占用时返回 OK()==true 且 Delay()==0，令牌已扣
// （调用者须 Commit 确认或 Cancel 归还，否则形成半占用残留）。不能立即占用时
// 返回 OK()==false 与正 Delay，此时不占用任何令牌，Reservation 亦不持有 limiter，
// 调用者等待 Delay 后重试即可——无需也不应 Cancel。
func (l *Limiter) Reserve(key string, n int) *Reservation {
	if n <= 0 {
		return &Reservation{}
	}
	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return &Reservation{}
	}
	q := l.quotaFor(key)
	l.mu.RUnlock()

	now := l.clk.Now()

	// peek 与 take 必须基于同一份状态，否则 DelayFor（只读）与 AllowN（内部重新
	// Load+refill）两次独立读取之间，桶可能被并发请求改动，出现 delay 与实际占用
	// 不一致。这里在循环内统一 Load+Take，CAS 失败则重试，保证语义原子。
	for attempt := 0; attempt < 8; attempt++ {
		l.mu.RLock()
		if l.closed {
			l.mu.RUnlock()
			return &Reservation{}
		}
		l.mu.RUnlock()

		st, ok, err := l.store.Load(key)
		if err != nil {
			return &Reservation{}
		}
		if !ok {
			st = bucket.NewState(q.Burst, now)
		}
		next, allowed, _ := bucket.Take(st, q.Rate, q.Burst, now, n)
		if !allowed {
			// 不足：仅基于当前状态告知需等待多久，不扣令牌、不持 limiter。
			delay := bucket.DelayFor(st, q.Rate, q.Burst, now, n)
			return &Reservation{ok: false, delay: delay}
		}
		if ok {
			swapped, err := l.store.CAS(key, st, next)
			if err != nil {
				return &Reservation{}
			}
			if !swapped {
				continue
			}
		} else {
			if err := l.store.Save(key, next); err != nil {
				return &Reservation{}
			}
		}
		return &Reservation{ok: true, tokens: n, delay: 0, limiter: l, key: key, held: true}
	}
	return &Reservation{}
}

func (r *Reservation) Cancel() {
	if r == nil || !r.held || r.limiter == nil {
		return
	}
	r.limiter.credit(r.key, r.tokens)
	r.held = false
	r.ok = false
}

// Commit 确认占用，取消归还义务。
func (r *Reservation) Commit() {
	if r == nil {
		return
	}
	r.held = false
}
