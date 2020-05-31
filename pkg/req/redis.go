package req

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

// TODO: make abstract redis layer
// Retrieve task body from storage
func (q *Q) getTaskFromHeap(ctx context.Context, id string) (*Task, error) {
	taskStr, err := q.client.Get(id).Result()
	if err != nil {
		return nil, errors.Wrap(err, "GET task by id")
	}
	q.logger.Debugf(ctx, "successfully got task itself %q from kv storage", taskStr)

	task := &Task{}
	if err := json.Unmarshal([]byte(taskStr), task); err != nil {
		// TODO: handle
		return nil, errors.Wrap(err, "decode task")
	}

	q.logger.Debugf(ctx, "successfully decoded task to %v", task)
	return task, nil
}

func (q *Q) putTaskToHeap(ctx context.Context, task *Task) error {
	taskStr, err := json.Marshal(&task)
	if err != nil {
		return errors.Wrap(err, "put: encode task")
	}

	// Set key-val
	if err := q.client.Set(task.Id, string(taskStr), 0).Err(); err != nil {
		// TODO: handle error
		return errors.Wrap(err, "put: SET")
	}

	return nil
}

func (q *Q) putTaskIdToDelayedTree(ctx context.Context, id string, delay time.Duration) error {
	if err := q.client.ZAdd(keyTreeDelayed(q.id), &redis.Z{
		Score:  float64(time.Now().Add(delay).Unix()),
		Member: id,
	}).Err(); err != nil {
		// TODO: handle error
		return errors.Wrap(err, "put: ZADD")
	}
	q.logger.Debugf(ctx, "successfully put task %q to delayed tree", id)
	return nil
}
