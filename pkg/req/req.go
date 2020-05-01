package req

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
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

type Config struct {
	Addr     string
	Password string
	EnableLogger bool
}

func Connect(ctx context.Context, cfg *Config) (*Q, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       0,
	})

	status := client.Ping()
	if status.Err() != nil {
		return nil, errors.Wrap(status.Err(), "ping redis")
	}

	q := &Q{
		client: client,
		locker:redislock.New(client),
	}

	q.logger = &defaultLogger{}
	if !cfg.EnableLogger {
		log.SetOutput(ioutil.Discard)
	}

	go q.traverseDelayed(ctx)

	return q, nil
}

type Q struct {
	client *redis.Client
	locker *redislock.Client
	logger Logger
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

		q.logger.Debug(ctx,"traverse delayed: trying to obtain lock")
		lock, err := q.locker.Obtain(lockTreeDelayed, 1*time.Minute, nil)
		if err != nil {
			if err == redislock.ErrNotObtained {
				q.logger.Error(ctx, "traverse delayed: ERR_NOT_OBTAINED")
			}
			q.logger.Errorf(ctx, "traverse delayed: lock tree: %v", err)
			time.Sleep(retryTimeout())
			continue
		}
		q.logger.Debug(ctx,"traverse delayed: obtained lock")

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
			time.Sleep(1*time.Second)
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
		time.Sleep(1*time.Second)
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
		Id: taskId,
		Delay: delay,
		Body:payload,
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
		taskId, err := q.client.BRPopLPush(listReady, listTaken, 10*time.Second).Result()
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

	if err := q.client.Del(id).Err(); err != nil {
		// TODO retry error
		return errors.Wrap(err, "ack: DEL")
	}

	return nil
}
