package req

import (
	"context"
	"strconv"
	"time"

	"github.com/kilchik/req/internal/pkg/storage"
	"github.com/pkg/errors"
)

// Runs at most once in period
func (q *Q) validateTaken(ctx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	for {
		select {
		case <-ctx.Done():
			q.logger.Debugf(ctx, "validate taken: done")
			return

		// Transfer task id from taken list back to ready in case of timeout
		case <-ticker.C:
			func() {
				var releaseLock func() error
				for {
					var err error
					releaseLock, err = q.store.TryLockBeforeValidatingTaken(ctx, q.id, period)
					if err == nil {
						break
					}
					if err == storage.ErrorLocked {
						// If another pod is already validating taken tasks
						time.Sleep(100 * time.Millisecond)
					} else {
						q.logger.Errorf(ctx, "validate taken: lock tree: %v", err)
					}
				}

				defer func() {
					if releaseLock != nil {
						if err := releaseLock(); err != nil {
							q.logger.Errorf(ctx, "validate taken: release lock: %v", err)
						}
					}
				}()

				// Check last validation timestamp to make sure that multiple clients do not run this check more than once in period
				t, err := q.getLastValidationTime(ctx)
				if err == nil && t.Add(period).After(time.Now()) {
					q.logger.Infof(ctx, "validate taken: not enough time has passed since last traversal")
					return
				}

				// Traverse all taken and compare if they are taken for too long
				size, err := q.store.GetTakenListLen(ctx, q.id)
				if err != nil {
					q.logger.Errorf(ctx, "validate taken: get taken list length: %v", err)
					return
				}
				if size == 0 {
					return
				}

				res, err := q.store.GetTakenSlice(ctx, q.id, 1024)
				if err != nil {
					q.logger.Errorf(ctx, "validate taken: get taken list: %v", err)
					return
				}
				for _, taskId := range res {
					t, err := q.store.GetTaskFromHeap(ctx, taskId)
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
				if err := q.store.SetValidationTimestamp(ctx, q.id); err != nil {
					q.logger.Errorf(ctx, "validate taken: update validation timestamp: %v", err)
				}
			}()
		}
	}
}

func (q *Q) getLastValidationTime(ctx context.Context) (*time.Time, error) {
	tsStr, err := q.store.GetValidationTimestamp(ctx, q.id)
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
	if err := q.store.PutTaskIdToReadyList(ctx, q.id, taskId); err != nil {
		return errors.Wrap(err, "put task id to ready list")
	}
	if err := q.store.DropTaskIdFromTakenList(ctx, q.id, taskId); err != nil {
		return errors.Wrap(err, "drop task id from taken list")
	}
	return nil
}
