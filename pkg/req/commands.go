package req

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/kilchik/req/internal/pkg/storage"
	"github.com/kilchik/req/pkg/types"
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

	task := &types.Task{
		Id:    taskId,
		Delay: delay,
		Body:  payload,
	}

	if err := q.store.PutTaskToHeap(ctx, task); err != nil {
		return "", errors.Wrap(err, "put task to heap")
	}
	q.logger.Debugf(ctx, "successfully put task %q to kv storage", taskId)

	if delay > 0 {
		if err := q.store.PutTaskIdToDelayedTree(ctx, q.id, taskId, delay); err != nil {
			return "", errors.Wrap(err, "put task id to delayed tree")
		}
	} else {
		// Lpush task id to ready list
		if err := q.store.PutTaskIdToReadyList(ctx, q.id, taskId); err != nil {
			return "", errors.Wrap(err, "put task id to ready list")
		}
		q.logger.Debugf(ctx, "successfully put task %q to ready list", taskId)
	}

	return taskId, nil
}

// Take returns next task from queue in FIFO order. If queue is empty the call will be blocked until any task is put
// into queue or context is canceled. The task will not be automatically removed or acknowledged by this action.
func (q *Q) Take(ctx context.Context, obj interface{}) (id string, err error) {
	const checkPeriod = 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return "", context.Canceled
		default:
		}

		// Transfer task id from ready list to taken list
		taskId, err := q.store.MoveTaskIdFromReadyListToTaken(ctx, q.id)
		if err != nil {
			if errors.Cause(err) == storage.ErrorNotFound {
				time.Sleep(checkPeriod)
				continue
			}
			return "", errors.Wrap(err, "move task id from ready list to taken")
		}
		q.logger.Debugf(ctx, "successfully got task id %q from ready list", taskId)

		task, err := q.store.GetTaskFromHeap(ctx, taskId)
		if err != nil {
			return "", errors.Wrap(err, "get task by id")
		}

		// Set time when task was taken
		task.TakenAt = time.Now()
		if err := q.store.PutTaskToHeap(ctx, task); err != nil {
			return "", errors.Wrap(err, "put task to heap")
		}

		// Extract payload
		if err := json.Unmarshal(task.Body, &obj); err != nil {
			return "", errors.Wrap(err, "decode task body")
		}

		return taskId, nil
	}
}

// Ack acknowledges task completion.
func (q *Q) Ack(ctx context.Context, id string) error {
	if err := q.store.DropTaskIdFromTakenList(ctx, q.id, id); err != nil {
		return errors.Wrap(err, "drop task id from taken list")
	}

	if err := q.store.IncrementDoneCounter(ctx, q.id); err != nil {
		return errors.Wrap(err, "increment done counter")
	}

	if err := q.store.DropTaskFromHeap(ctx, id); err != nil {
		return errors.Wrap(err, "drop task body")
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
	res.Ready, err = q.store.GetReadyListLen(ctx, q.id)
	if err != nil {
		return nil, errors.Wrap(err, "get ready list size")
	}
	res.Taken, err = q.store.GetTakenListLen(ctx, q.id)
	if err != nil {
		return nil, errors.Wrap(err, "get taken list size")
	}
	res.Delayed, err = q.store.GetDelayedTreeSize(ctx, q.id)
	if err != nil {
		return nil, errors.Wrap(err, "get delayed tree size")
	}
	res.Done, err = q.store.GetDoneCounterValue(ctx, q.id)
	if err != nil {
		return nil, errors.Wrap(err, "get done counter value")
	}
	res.Buried, err = q.store.GetBuriedSetSize(ctx, q.id)
	if err != nil {
		return nil, errors.Wrap(err, "get buried set size")
	}
	return res, nil
}

// Delete removes task from queue if it is currently taken.
func (q *Q) Delete(ctx context.Context, id string) error {
	if err := q.store.DropTaskIdFromTakenList(ctx, q.id, id); err != nil {
		return errors.Wrap(err, "drop task id from taken list")
	}
	if err := q.store.DropTaskFromHeap(ctx, id); err != nil {
		return errors.Wrapf(err, "drop task body")
	}
	return nil
}

// DeleteBuried removes task from buried set.
func (q *Q) DeleteBuried(ctx context.Context, id string) error {
	if err := q.store.DropTaskIdFromBuriedSet(ctx, q.id, id); err != nil {
		return errors.Wrap(err, "drop task id from buried set")
	}
	if err := q.store.DropTaskFromHeap(ctx, id); err != nil {
		return errors.Wrapf(err, "drop task body")
	}
	return nil
}

func newExpDelayWithJitter(delayCur, delayMin, delayMax time.Duration) time.Duration {
	if delayCur == 0 {
		return delayMin
	}
	delayNext := delayCur + time.Duration(float64(delayCur)*(rand.Float64()+0.5))
	if delayNext > delayMax {
		return delayMax
	}
	return delayNext
}

// Delay sets new delay before task can be taken. If the task was delayed once then next delay period will be somewhat
// twice longer but no longer than an hour. The delay is set to 1 second for the first time.
func (q *Q) Delay(ctx context.Context, taskId string) error {
	return q.DelayCustom(ctx, taskId, 1*time.Second, 1*time.Hour)
}

// Delay sets new custom delay in interval between "from" and "to" before task can be taken. If the task was delayed
// once then next delay period will be somewhat twice longer but no longer than "to".
func (q *Q) DelayCustom(ctx context.Context, id string, from, to time.Duration) error {
	task, err := q.store.GetTaskFromHeap(ctx, id)
	if err != nil {
		return errors.Wrapf(err, "get task by id %q", id)
	}
	q.logger.Debugf(ctx, "task %q has delay %v", task.Delay)
	task.Delay = newExpDelayWithJitter(task.Delay, from, to)
	if err := q.store.PutTaskToHeap(ctx, task); err != nil {
		return errors.Wrap(err, "put task to heap")
	}
	q.logger.Debugf(ctx, "putting task %q with delay %v", task.Delay)
	if err := q.store.PutTaskIdToDelayedTree(ctx, q.id, id, task.Delay); err != nil {
		return errors.Wrap(err, "put task id to delayed tree")
	}
	if err := q.store.DropTaskIdFromTakenList(ctx, q.id, id); err != nil {
		return errors.Wrap(err, "drop task id from taken list")
	}

	return nil
}

// Move task id from taken list to buried set
func (q *Q) Bury(ctx context.Context, id string) error {
	if err := q.store.PutTaskIdToBuriedSet(ctx, q.id, id); err != nil {
		return errors.Wrap(err, "put task id to buried set")
	}
	if err := q.store.DropTaskIdFromTakenList(ctx, q.id, id); err != nil {
		return errors.Wrap(err, "drop task id from taken list")
	}
	return nil
}

// Move task id from buried set to ready list
func (q *Q) Kick(ctx context.Context, taskId string) error {
	if err := q.store.PutTaskIdToReadyList(ctx, q.id, taskId); err != nil {
		return errors.Wrap(err, "put task id to ready list")
	}
	if err := q.store.DropTaskIdFromBuriedSet(ctx, q.id, taskId); err != nil {
		return errors.Wrap(err, "drop task id from buried set")
	}
	return nil
}

// Move all task ids from buried set to ready list
func (q *Q) KickAll(ctx context.Context) error {
	var releaseLock func() error
	for {
		var err error
		releaseLock, err = q.store.TryLockBeforeKick(ctx, q.id, 1*time.Minute)
		if err == nil {
			break
		}
		if err == storage.ErrorLocked {
			// If another pod is already kicking tasks
			time.Sleep(1 * time.Second)
		} else {
			return errors.Wrap(err, "kick all: obtain lock")
		}
	}

	defer func() {
		if releaseLock != nil {
			if err := releaseLock(); err != nil {
				q.logger.Errorf(ctx, "kick all: release lock: %v", err)
			}
		}
	}()

	for {
		taskId, err := q.store.GetRandomBuriedTaskId(ctx, q.id)
		if err != nil {
			if errors.Cause(err) == storage.ErrorNotFound {
				break
			}
			return errors.Wrap(err, "get random task id from buried set")
		}
		if err := q.Kick(ctx, taskId); err != nil {
			return errors.Wrapf(err, "kick %q", taskId)
		}
	}
	return nil
}

func (q *Q) Top(ctx context.Context, state TaskState, size int64) ([]string, error) {
	switch state {
	case StateDelayed:
		return q.store.GetDelayedSlice(ctx, q.id, size)
	case StateReady:
		return q.store.GetReadySlice(ctx, q.id, size)
	case StateTaken:
		return q.store.GetTakenSlice(ctx, q.id, size)
	case StateBuried:
		return q.store.GetBuriedSlice(ctx, q.id, size)
	default:
		return nil, fmt.Errorf("unexpected state")
	}
}

func (q *Q) Watch(ctx context.Context, tid string) (*types.Task, error) {
	return q.store.GetTaskFromHeap(ctx, tid)
}

func (q *Q) Find(ctx context.Context, pattern string) (map[TaskState][]*types.Task, error) {
	res := make(map[TaskState][]*types.Task)
	tasks, err := q.store.FindInHeap(ctx, q.id, pattern)
	if err != nil {
		return nil, errors.Wrap(err, "find in heap")
	}

	for _, task := range tasks {
		isBuried, err := q.store.IsTaskBuried(ctx, q.id, task.Id)
		if err != nil {
			return nil, errors.Wrap(err, "check if task is buried")
		}
		if isBuried {
			if _, exists := res[StateBuried]; !exists {
				res[StateBuried] = []*types.Task{}
			}
			res[StateBuried] = append(res[StateBuried], task)
			continue
		}

		isDelayed, err := q.store.IsTaskDelayed(ctx, q.id, task.Id)
		if err != nil {
			return nil, errors.Wrap(err, "check if task is delayed")
		}
		if isDelayed {
			if _, exists := res[StateDelayed]; !exists {
				res[StateDelayed] = []*types.Task{}
			}
			res[StateDelayed] = append(res[StateDelayed], task)
			continue
		}

		isReady, err := q.store.IsTaskReady(ctx, q.id, task.Id)
		if err != nil {
			return nil, errors.Wrap(err, "check if task is ready")
		}
		if isReady {
			if _, exists := res[StateReady]; !exists {
				res[StateReady] = []*types.Task{}
			}
			res[StateReady] = append(res[StateReady], task)
			continue
		}

		isTaken, err := q.store.IsTaskTaken(ctx, q.id, task.Id)
		if err != nil {
			return nil, errors.Wrap(err, "check if task is taken")
		}
		if isTaken {
			if _, exists := res[StateTaken]; !exists {
				res[StateTaken] = []*types.Task{}
			}
			res[StateTaken] = append(res[StateTaken], task)
			continue
		}
	}

	return res, nil
}
