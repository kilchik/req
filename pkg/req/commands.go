package req

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Put puts object of any type into queue and returns unique identifier for it.
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

	if err := q.putTaskToHeap(ctx, task); err != nil {
		return "", errors.Wrap(err, "put task to heap")
	}
	q.logger.Debugf(ctx, "successfully put task %q to kv storage", taskId)

	if delay > 0 {
		if err := q.putTaskIdToDelayedTree(ctx, taskId, delay); err != nil {
			return "", errors.Wrap(err, "put task id to delayed tree")
		}
	} else {
		// Lpush task id to ready list
		if err := q.client.LPush(keyListReady(q.id), taskId).Err(); err != nil {
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

// Take returns next task from queue in FIFO order. If queue is empty the call will be blocked until any task is put
// into queue or context is canceled. The task will not be automatically removed or acknowledged by this action.
func (q *Q) Take(ctx context.Context, obj interface{}) (id string, err error) {
	retryTimeout := timeoutExp()
	for {
		select {
		case <-ctx.Done():
			return "", context.Canceled
		default:
		}

		// Transfer task id from ready list to taken list
		taskId, err := q.client.BRPopLPush(keyListReady(q.id), keyListTaken(q.id), 1*time.Second).Result()
		if err != nil {
			// If timeout
			if err == redis.Nil {
				continue
			}

			time.Sleep(retryTimeout())
			continue
		}
		q.logger.Debugf(ctx, "successfully got task id %q from ready list", taskId)

		task, err := q.getTaskFromHeap(ctx, taskId)
		if err != nil {
			return "", errors.Wrap(err, "get task by id")
		}
		if task.Id != taskId {
			// TODO: handle
			return "", errors.New("smth bad happened")
		}

		// Set time when task was taken
		task.TakenAt = time.Now()
		if err := q.putTaskToHeap(ctx, task); err != nil {
			return "", errors.Wrap(err, "put task to heap")
		}

		// Extract payload
		if err := json.Unmarshal(task.Body, &obj); err != nil {
			return "", errors.Wrap(err, "decode task body")
		}
		q.logger.Debugf(ctx, "successfully decoded task body")

		return taskId, nil
	}
}

// Ack acknowledges task completion.
func (q *Q) Ack(ctx context.Context, id string) error {
	if err := q.client.LRem(keyListTaken(q.id), -1, id).Err(); err != nil {
		// TODO: retry error
		return errors.Wrap(err, "ack: LREM")
	}

	if err := q.client.Incr(keyCounterDone(q.id)).Err(); err != nil {
		q.logger.Errorf(ctx, "ack: INCR done counter: %v", err)
	}

	if err := q.client.Del(id).Err(); err != nil {
		// TODO retry error
		return errors.Wrap(err, "ack: DEL")
	}

	return nil
}

type Stat struct {
	Ready, Taken, Delayed, Done, Buried int64
}

// Stat returns statistics on tasks currently present in tree and number of done tasks overall.
func (q *Q) Stat(ctx context.Context) (*Stat, error) {
	res := &Stat{}
	var err error
	res.Ready, err = q.client.LLen(keyListReady(q.id)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: LLEN ready")
	}
	res.Taken, err = q.client.LLen(keyListTaken(q.id)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: LLEN taken")
	}
	res.Delayed, err = q.client.ZCount(keyTreeDelayed(q.id), "-inf", "+inf").Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: ZCOUNT delayed")
	}
	var doneStr string
	doneStr, err = q.client.Get(keyCounterDone(q.id)).Result()
	if err != nil {
		if err != redis.Nil {
			return nil, errors.Wrap(err, "stat: GET done")
		}
		res.Done = 0
	} else {
		res.Done, err = strconv.ParseInt(doneStr, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "stat: parse done string")
		}
	}
	res.Buried, err = q.client.SCard(keySetBuried(q.id)).Result()
	if err != nil {
		return nil, errors.Wrap(err, "stat: SCARD buried")
	}
	return res, nil
}

// Delete removes task from queue if it is currently taken.
func (q *Q) Delete(ctx context.Context, taskId string) error {
	deleted, err := q.client.LRem(keyListTaken(q.id), 1, taskId).Result()
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

func newExpDelayWithJitter(delay time.Duration) time.Duration {
	if delay == 0 {
		return 1 * time.Second
	}
	return delay + time.Duration(float64(delay)*(rand.Float64()+0.5))
}

// Delay sets new delay before task can be taken. If task was delayed
func (q *Q) Delay(ctx context.Context, taskId string) error {
	task, err := q.getTaskFromHeap(ctx, taskId)
	if err != nil {
		return errors.Wrapf(err, "get task by id %q", taskId)
	}
	task.Delay = newExpDelayWithJitter(task.Delay)
	if err := q.putTaskToHeap(ctx, task); err != nil {
		return errors.Wrap(err, "put task to heap")
	}
	if err := q.putTaskIdToDelayedTree(ctx, taskId, task.Delay); err != nil {
		return errors.Wrap(err, "put task id to delayed tree")
	}
	if err := q.client.LRem(keyListTaken(q.id), -1, taskId).Err(); err != nil {
		// TODO: retry error
		return errors.Wrap(err, "delay: LREM")
	}
	return nil
}

// Move task id from taken list to buried set
func (q *Q) Bury(ctx context.Context, taskId string) error {
	if err := q.client.SAdd(keySetBuried(q.id), taskId).Err(); err != nil {
		return errors.Wrap(err, "bury: SADD to buried set")
	}
	if err := q.client.LRem(keyListTaken(q.id), 1, taskId).Err(); err != nil {
		return errors.Wrap(err, "bury: LREM from taken list")
	}
	return nil
}

// Move task id from buried set to ready list
func (q *Q) Kick(ctx context.Context, taskId string) error {
	if err := q.client.LPush(keyListReady(q.id), taskId).Err(); err != nil {
		return errors.Wrap(err, "kick: LPUSH task id to ready list")
	}
	if err := q.client.SRem(keySetBuried(q.id), taskId).Err(); err != nil {
		return errors.Wrap(err, "kick: SREM task id from buried set")
	}
	return nil
}

// Move all task ids from buried set to ready list
func (q *Q) KickAll(ctx context.Context) error {
	lock, err := q.locker.Obtain(lockKickAllInProgress(q.id), 10*time.Minute, nil)
	if err != nil {
		return errors.Wrap(err, "kick all: obtain lock")
	}
	defer lock.Release()
	for {
		taskId, err := q.client.SRandMember(keySetBuried(q.id)).Result()
		if err != nil {
			if err == redis.Nil {
				break
			}
			return errors.Wrap(err, "kick all: SRANDMEMBER")
		}
		if err := q.Kick(ctx, taskId); err != nil {
			return errors.Wrapf(err, "kick all: kick %q", taskId)
		}
	}
	return nil
}
