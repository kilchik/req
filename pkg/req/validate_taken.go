package req

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	"github.com/pkg/errors"
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
					// If another pod is already validating taken tasks
					q.logger.Warn(ctx, "validate taken: ERR_NOT_OBTAINED")
				} else {
					q.logger.Warnf(ctx, "validate taken: lock tree: %v", err)
				}
				continue
			}

			// Check last validation timestamp to make sure that multiple clients do not run this check more than once in period
			t, err := q.getLastValidationTime()
			if err == nil && t.Add(period).After(time.Now()) {
				q.logger.Infof(ctx, "validate taken: not enough time has passed since last traversal")
				continue
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
				t, err := q.getTaskFromHeap(ctx, taskId)
				if err != nil {
					q.logger.Errorf(ctx, "validate taken: get task from heap: %v", err)
					continue
				}
				timeDefault := time.Time{}
				if t.TakenAt != timeDefault && t.TakenAt.Before(time.Now().Add(-q.takeTimeout)) {
					q.logger.Errorf(ctx, "validate taken: task %q was taken at %v; moving it back to 'ready' list...", taskId, t.TakenAt)
					if err := q.moveTakenToReady(ctx, taskId); err != nil {
						q.logger.Errorf(ctx, "validate taken: move task %q from taken to ready list: %v", err)
						continue
					}
				}
			}
			q.client.Set(keyLastValidationTs(q.id), fmt.Sprintf("%d", time.Now().Unix()), 0)
			lock.Release()
		}
	}
}

func (q *Q) getLastValidationTime() (*time.Time, error) {
	tsStr, err := q.client.Get(keyLastValidationTs(q.id)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "GET last validation ts")
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "convert ts string to int")
	}
	t := time.Unix(ts, 0)
	return &t, nil
}

func (q *Q) moveTakenToReady(ctx context.Context, taskId string) error {
	if err := q.client.LPush(keyListReady(q.id), taskId).Err(); err != nil {
		return errors.Wrap(err, "LPUSH to ready list")
	}
	if err := q.client.LRem(keyListTaken(q.id), 1, taskId).Err(); err != nil {
		return errors.Wrap(err, "LREM from taken list")
	}
	return nil
}
