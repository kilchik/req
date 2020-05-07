package req

import (
	"context"
	"time"

	"github.com/bsm/redislock"
)

func (q *Q) traverseDelayed(ctx context.Context) {
	retryTimeout := timeoutExp()
	for {
		select {
		case <-ctx.Done():
			q.logger.Debug(ctx, "done traversing")
			return
		default:
		}

		q.logger.Debug(ctx, "traverse delayed: trying to obtain lock")
		lock, err := q.locker.Obtain(lockTreeDelayed(q.id), 1*time.Minute, nil)
		if err != nil {
			if err == redislock.ErrNotObtained {
				q.logger.Error(ctx, "traverse delayed: ERR_NOT_OBTAINED")
			}
			q.logger.Errorf(ctx, "traverse delayed: lock tree: %v", err)
			time.Sleep(retryTimeout())
			continue
		}
		q.logger.Debug(ctx, "traverse delayed: obtained lock")

		res, err := q.client.ZRangeWithScores(keyTreeDelayed(q.id), 0, 0).Result()
		if err != nil {
			q.logger.Errorf(ctx, "traverse delayed: ZRANGEWITHSCORES: %v", err)
			lock.Release()
			time.Sleep(retryTimeout())
			continue
		}

		if len(res) == 0 {
			q.logger.Debug(ctx, "traverse delayed: no delayed tasks found")
			lock.Release()
			time.Sleep(1 * time.Second)
			retryTimeout = timeoutExp()
			continue
		}

		q.logger.Debug(ctx, float64(time.Now().Unix()), res[0].Score)
		if float64(time.Now().Unix()) >= res[0].Score {
			q.logger.Debugf(ctx, "traverse delayed: pushing task %q to ready list", res[0].Member.(string))
			if err := q.client.LPush(keyListReady(q.id), res[0].Member.(string)).Err(); err != nil {
				q.logger.Errorf(ctx, "traverse delayed: LPUSH: %v", err)
				lock.Release()
				time.Sleep(retryTimeout())
				continue
			}
			if err := q.client.ZPopMin(keyTreeDelayed(q.id), 1).Err(); err != nil {
				q.logger.Errorf(ctx, "traverse delayed: ZPOPMIN: %v", err)
				lock.Release()
				time.Sleep(retryTimeout())
				continue
			}
			lock.Release()
			retryTimeout = timeoutExp()
			continue
		}

		q.logger.Debugf(ctx, "traverse delayed: delayed task %q is not ready yet", res[0].Member.(string))
		lock.Release()
		retryTimeout = timeoutExp()
		time.Sleep(1 * time.Second)
	}
}
