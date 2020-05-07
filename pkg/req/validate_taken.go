package req

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/bsm/redislock"
)

// Runs at most once in period
func (q *Q) validateTaken(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	var lock *redislock.Lock
	for {
		select {
		case <-ctx.Done():
			q.logger.Debugf(ctx, "validate taken: done")
			if lock != nil {
				lock.Release()
			}
			return

		case <-ticker.C:
			// Transfer task id from taken list back to ready in case of timeout
			// TODO: wrap with retry func
			var err error
			lock, err = q.locker.Obtain(lockTakenValidation(q.id), period, nil)
			if err != nil {
				if err == redislock.ErrNotObtained {
					// If another pod is already validating taken
					q.logger.Warn(ctx, "validate taken: ERR_NOT_OBTAINED")
				} else {
					q.logger.Warnf(ctx, "validate taken: lock tree: %v", err)
				}
				continue
			}

			// Check last validation timestamp
			tsStr, err := q.client.Get(keyLastValidationTs(q.id)).Result()
			if err == nil {
				ts, err := strconv.ParseInt(tsStr, 10, 64)
				if err != nil {
					q.logger.Errorf(ctx, "validate taken: convert ts string to int: %v", err)
					continue
				}
				if time.Unix(ts, 0).Add(period).Before(time.Now()) {
					q.logger.Infof(ctx, "validate taken: not enough time has passed since last traversal")
					continue
				}
			}

			// Traverse all taken and compare if they are taken for too long
			size, err := q.client.LLen(keyListTaken(q.id)).Result()
			if err != nil {
				q.logger.Errorf(ctx, "validate taken: LLEN taken list: %v", err)
				continue
			}
			if size == 0 {
				lock.Release()
				continue
			}
			if size > 1024 {
				// TODO: alert
				q.logger.Errorf(ctx, "validate taken: taken list is too long: %d items", size)
			}
			res, err := q.client.LRange(keyListTaken(q.id), 0, 1024).Result()
			if err != nil {
				q.logger.Errorf(ctx, "validate taken: LRANGE: %v", err)
				continue
			}
			for _, taskId := range res {
				task, err := q.client.Get(taskId).Result()
				if err != nil {
					q.logger.Errorf(ctx, "validate taken: GET %q: %v", err)
					continue
				}
				t := &Task{}
				if err := json.Unmarshal([]byte(task), t); err != nil {
					q.logger.Errorf(ctx, "validate taken: decode task: %v", err)
					continue
				}
				timeDefault := time.Time{}
				if t.TakenAt != timeDefault && t.TakenAt.Before(time.Now().Add(-q.takeTimeout)) {
					q.logger.Errorf(ctx, "validate taken: task %q was taken at %v; moving it back to 'ready' list...", taskId, t.TakenAt)
					if err := q.client.LPush(keyListReady(q.id), taskId).Err(); err != nil {
						q.logger.Errorf(ctx, "validate taken: put task %q back to ready list: %v", err)
						// TODO: handle error
						continue
					}
					if err := q.client.LRem(keyListTaken(q.id), 1, taskId).Err(); err != nil {
						// TODO: retry and handle
						q.logger.Errorf(ctx, "validate taken: LREM task %q from taken list: %v", taskId, err)
						continue
					}
				}
			}
			q.client.Set(keyLastValidationTs(q.id), fmt.Sprintf("%d", time.Now().Unix()), 0)
			lock.Release()
		}
	}
}
