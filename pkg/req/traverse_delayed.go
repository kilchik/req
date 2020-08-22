package req

import (
	"context"
	"time"

	"github.com/kilchik/req/internal/pkg/storage"
)

func (q *Q) traverseDelayed(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	retryTimeout := timeoutExp()
	for {
		select {
		case <-ctx.Done():
			q.logger.Debug(ctx, "done traversing")
			return

		// Transfer task id from taken list back to ready in case of timeout
		case <-ticker.C:
			func() {
				var releaseLock func() error
				for {
					var err error
					releaseLock, err = q.store.TryLockBeforeTraversingDelayed(ctx, q.id, 1*time.Minute)
					if err == nil {
						break
					}
					if err == storage.ErrorLocked {
						// If another pod is already traversing delayed tasks
						time.Sleep(retryTimeout())
					} else {
						q.logger.Errorf(ctx, "traverse delayed: lock tree: %v", err)
					}
				}

				defer func() {
					if releaseLock != nil {
						if err := releaseLock(); err != nil {
							q.logger.Errorf(ctx, "traverse delayed: release lock: %v", err)
						}
					}
				}()

				q.logger.Debug(ctx, "traverse delayed: obtained lock")
				tid, delayTill, err := q.store.GetDelayedHead(ctx, q.id)
				if err != nil {
					if err != storage.ErrorNotFound {
						q.logger.Errorf(ctx, "traverse delayed: get delayed head: %v", err)
					}
					return
				}

				if float64(time.Now().Unix()) >= delayTill {
					q.logger.Debugf(ctx, "traverse delayed: pushing task %q to ready list", tid)
					if err := q.store.PutTaskIdToReadyList(ctx, q.id, tid); err != nil {
						q.logger.Errorf(ctx, "traverse delayed: put task id to ready list: %v", err)
						return
					}
					if err := q.store.DropTaskIdFromDelayedTree(ctx, q.id, tid); err != nil {
						q.logger.Errorf(ctx, "traverse delayed: drop task %q from taken list: %v", tid, err)
						return
					}
					return
				}

				q.logger.Debugf(ctx, "traverse delayed: delayed task %q is not ready yet", tid)
			}()
		}
	}
}
