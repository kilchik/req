package req

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	listReady = "req_list_ready"
	listTaken = "req_list_taken"
)

const treeDelayed = "req_tree_delayed"

const lockTreeDelayed = "req_lock_tree_delayed"

const lockTakenValidation = "req_lock_taken_validation"
const lastValidationTimestamp = "req_last_validation"

const counterDone = "req_count_done"

type Q struct {
	client *redis.Client
	locker *redislock.Client
	logger Logger

	takeTimeout           time.Duration
	takenValidationPeriod time.Duration
}

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
		lock, err := q.locker.Obtain(lockTreeDelayed, 1*time.Minute, nil)
		if err != nil {
			if err == redislock.ErrNotObtained {
				q.logger.Error(ctx, "traverse delayed: ERR_NOT_OBTAINED")
			}
			q.logger.Errorf(ctx, "traverse delayed: lock tree: %v", err)
			time.Sleep(retryTimeout())
			continue
		}
		q.logger.Debug(ctx, "traverse delayed: obtained lock")

		res, err := q.client.ZRangeWithScores(treeDelayed, 0, 0).Result()
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
			if err := q.client.LPush(listReady, res[0].Member.(string)).Err(); err != nil {
				q.logger.Errorf(ctx, "traverse delayed: LPUSH: %v", err)
				lock.Release()
				time.Sleep(retryTimeout())
				continue
			}
			if err := q.client.ZPopMin(treeDelayed, 1).Err(); err != nil {
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

func (q *Q) Put(ctx context.Context, obj interface{}, delay time.Duration) (taskId string, err error) {
	// Serialize obj
	payload, err := json.Marshal(obj)
	if err != nil {
		return "", errors.Wrap(err, "put: encode obj")
	}

	// Gen unique task id
	taskId = uuid.New().String()

	task := &Task{
		Id:    taskId,
		Delay: delay,
		Body:  payload,
	}

	taskStr, err := json.Marshal(&task)
	if err != nil {
		return "", errors.Wrap(err, "put: encode task")
	}

	// Set key-val
	if err := q.client.Set(taskId, string(taskStr), 0).Err(); err != nil {
		// TODO: handle error
		return "", errors.Wrap(err, "put: SET")
	}
	q.logger.Debugf(ctx, "successfully put task %q to kv storage", taskId)

	if delay > 0 {
		q.logger.Debug(ctx, float64(time.Now().Add(delay).Unix()))
		if err := q.client.ZAdd(treeDelayed, &redis.Z{
			Score:  float64(time.Now().Add(delay).Unix()),
			Member: taskId,
		}).Err(); err != nil {
			// TODO: handle error
			return "", errors.Wrap(err, "put: ZADD")
		}
		q.logger.Debugf(ctx, "successfully put task %q to delayed tree", taskId)
	} else {
		// Lpush task id to ready list
		if err := q.client.LPush(listReady, taskId).Err(); err != nil {
			// TODO: handle error
			return "", errors.Wrap(err, "put: LPUSH")
		}
		q.logger.Debugf(ctx, "successfully put task %q to ready list", taskId)
	}

	return taskId, nil
}

func timeoutExp() func() time.Duration {
	timeout := time.Second
	return func() time.Duration {
		timeout *= 2
		return timeout
	}
}

func (q *Q) Take(ctx context.Context, obj interface{}) (id string, err error) {
	retryTimeout := timeoutExp()
	for {
		select {
		case <-ctx.Done():
			return "", context.Canceled
		default:
		}

		// Transfer task id from ready list to taken list
		taskId, err := q.client.BRPopLPush(listReady, listTaken, 1*time.Second).Result()
		if err != nil {
			// If timeout
			if err == redis.Nil {
				continue
			}

			log.Printf("take: BRPOPLPUSH: %v", err)
			time.Sleep(retryTimeout())
			continue
		}
		q.logger.Debugf(ctx, "successfully got task id %q from ready list", taskId)

		// Retrieve task body from storage
		taskStr, err := q.client.Get(taskId).Result()
		q.logger.Debugf(ctx, "successfully got task itself %q from kv storage", taskStr)
		task := &Task{}
		if err := json.Unmarshal([]byte(taskStr), task); err != nil {
			// TODO: handle
			return "", errors.Wrap(err, "decode task")
		}
		q.logger.Debugf(ctx, "successfully decoded task to %v", task)
		if task.Id != taskId {
			// TODO: handle
			return "", errors.New("smth bad happened")
		}

		// Set time when task was taken
		task.TakenAt = time.Now()
		taskNewStr, _ := json.Marshal(task)
		q.client.Set(taskId, taskNewStr, 0)

		// Extract payload
		if err := json.Unmarshal(task.Body, &obj); err != nil {
			return "", errors.Wrap(err, "decode task body")
		}
		q.logger.Debugf(ctx, "successfully decoded task body")

		return taskId, nil
	}
}

func (q *Q) Ack(ctx context.Context, id string) error {
	if err := q.client.LRem(listTaken, -1, id).Err(); err != nil {
		// TODO: retry error
		return errors.Wrap(err, "ack: LREM")
	}

	if err := q.client.Incr(counterDone).Err(); err != nil {
		q.logger.Errorf(ctx, "ack: INCR done counter: %v", err)
	}

	if err := q.client.Del(id).Err(); err != nil {
		// TODO retry error
		return errors.Wrap(err, "ack: DEL")
	}

	return nil
}

type Stat struct {
	Ready, Taken, Delayed, Done int64
}

func (q *Q) Stat(ctx context.Context) (*Stat, error) {
	res := &Stat{}
	var err error
	res.Ready, err = q.client.LLen(listReady).Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: LLEN ready")
	}
	res.Taken, err = q.client.LLen(listTaken).Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: LLEN taken")
	}
	res.Delayed, err = q.client.ZCount(treeDelayed, "-inf", "+inf").Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: ZCOUNT delayed")
	}
	var doneStr string
	doneStr, err = q.client.Get(counterDone).Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: GET done")
	}
	res.Done, err = strconv.ParseInt(doneStr, 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "stat: parse done string")
	}
	return res, nil
}

// Deletes task if it is present in taken list
func (q *Q) Delete(ctx context.Context, taskId string) error {
	deleted, err := q.client.LRem(listTaken, 1, taskId).Result()
	if err != nil {
		return errors.Wrapf(err, "delete: LREM %q from taken list: %v", taskId, err)
	}
	if deleted != 1 {
		return errors.New(fmt.Sprintf("delete: no task with id %q found", taskId))
	}
	if err := q.client.Del(taskId).Err(); err != nil {
		return errors.Wrapf(err, "delete: DEL %q", taskId)
	}
	return nil
}
