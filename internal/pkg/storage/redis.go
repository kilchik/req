package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/kilchik/req/pkg/logger"
	"github.com/kilchik/req/pkg/types"
	"github.com/pkg/errors"
)

type Storage interface {
	// Queue id
	GetQId(ctx context.Context, name string) (string, error)
	SetQIdForName(ctx context.Context, id, name string) error

	// Heap
	PutTaskToHeap(ctx context.Context, task *types.Task) error
	GetTaskFromHeap(ctx context.Context, id string) (*types.Task, error)
	GetValidationTimestamp(ctx context.Context, qid string) (ts string, err error)
	SetValidationTimestamp(ctx context.Context, qid string) error
	DropTaskFromHeap(ctx context.Context, tid string) error
	FindInHeap(ctx context.Context, qid string, pattern string) ([]*types.Task, error)

	// Task id
	PutTaskIdToDelayedTree(ctx context.Context, qid, id string, delay time.Duration) error
	PutTaskIdToReadyList(ctx context.Context, qid, tid string) error
	PutTaskIdToBuriedSet(ctx context.Context, qid, tid string) error
	IsTaskBuried(ctx context.Context, qid, tid string) (bool, error)
	IsTaskDelayed(ctx context.Context, qid, tid string) (bool, error)
	IsTaskReady(ctx context.Context, qid, tid string) (bool, error)
	IsTaskTaken(ctx context.Context, qid, tid string) (bool, error)
	GetRandomBuriedTaskId(ctx context.Context, qid string) (tid string, err error)
	MoveTaskIdFromReadyListToTaken(ctx context.Context, qid string) (string, error)
	DropTaskIdFromTakenList(ctx context.Context, qid, tid string) error
	DropTaskIdFromDelayedTree(ctx context.Context, qid, tid string) error
	GetTakenSlice(ctx context.Context, qid string, size int64) ([]string, error)
	GetDelayedSlice(ctx context.Context, qid string, size int64) ([]string, error)
	GetReadySlice(ctx context.Context, qid string, size int64) ([]string, error)
	GetBuriedSlice(ctx context.Context, qid string, size int64) ([]string, error)
	DropTaskIdFromBuriedSet(ctx context.Context, qid, tid string) error
	GetDelayedHead(ctx context.Context, qid string) (tid string, delayScore float64, err error)

	// Size of structures
	GetReadyListLen(ctx context.Context, qid string) (int64, error)
	GetTakenListLen(ctx context.Context, qid string) (int64, error)
	GetDelayedTreeSize(ctx context.Context, qid string) (int64, error)
	GetBuriedSetSize(ctx context.Context, qid string) (int64, error)

	// Counters
	IncrementDoneCounter(ctx context.Context, qid string) error
	GetDoneCounterValue(ctx context.Context, qid string) (int64, error)

	// Locks
	TryLockBeforeValidatingTaken(ctx context.Context, qid string, period time.Duration) (func() error, error)
	TryLockBeforeTraversingDelayed(ctx context.Context, qid string, period time.Duration) (func() error, error)
	TryLockBeforeKick(ctx context.Context, qid string, period time.Duration) (func() error, error)
}

type StorageImpl struct {
	client *redis.Client
	locker *redislock.Client
	logger logger.Logger
}

const defaultRetryLimit = 5

func New(redisClient *redis.Client, locker *redislock.Client, logger logger.Logger) *StorageImpl {
	return &StorageImpl{redisClient, locker, logger}
}

func (r *StorageImpl) GetQId(ctx context.Context, name string) (string, error) {
	var id string
	err := retry(ctx, func() error {
		var err error
		id, err = r.client.Get(name).Result()
		if err != nil {
			if err == redis.Nil {
				return ErrorNotFound
			}
			r.logger.Errorf(ctx, "get queue id for name %q: %v", name, err)
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit)

	return id, err
}

func (r *StorageImpl) SetQIdForName(ctx context.Context, id, name string) error {
	return retry(ctx, func() error {
		if err := r.client.Set(name, id, 0).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit)
}

// Retrieve task body from storage
func (r *StorageImpl) GetTaskFromHeap(ctx context.Context, id string) (*types.Task, error) {
	var taskStr string
	if err := retry(ctx, func() error {
		var err error
		taskStr, err = r.client.Get(id).Result()
		if err != nil {
			if err == redis.Nil {
				return ErrorNotFound
			}
			r.logger.Errorf(ctx, "GET task by id %q: %v", id, err)
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return nil, errors.Wrapf(err, "get task body")
	}

	task := &types.Task{}
	if err := json.Unmarshal([]byte(taskStr), task); err != nil {
		return nil, errors.Wrap(err, "decode task")
	}

	return task, nil
}

// Retrieve validation timestamp from heap
func (r *StorageImpl) GetValidationTimestamp(ctx context.Context, qid string) (ts string, err error) {
	var tstamp string
	if err := retry(ctx, func() error {
		var err error
		tkey := keyLastValidationTs(qid)
		tstamp, err = r.client.Get(tkey).Result()
		if err != nil {
			if err == redis.Nil {
				return ErrorNotFound
			}
			r.logger.Errorf(ctx, "GET timestamp by key %q: %v", tkey, err)
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return "", errors.Wrapf(err, "get timestamp")
	}

	return tstamp, nil
}

// Upsert validation timestamp in heap
func (r *StorageImpl) SetValidationTimestamp(ctx context.Context, qid string) error {
	return retry(ctx, func() error {
		if err := r.client.Set(keyLastValidationTs(qid), fmt.Sprintf("%d", time.Now().Unix()), 0).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit)
}

// Put task body to storage
func (r *StorageImpl) PutTaskToHeap(ctx context.Context, task *types.Task) error {
	taskStr, err := json.Marshal(&task)
	if err != nil {
		return errors.Wrap(err, "put: encode task")
	}

	if err := retry(ctx, func() error {
		if err := r.client.Set(task.Id, string(taskStr), 0).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "SET task body")
	}

	return nil
}

// Drop task body from storage using DEL
func (r *StorageImpl) DropTaskFromHeap(ctx context.Context, tid string) error {
	if err := retry(ctx, func() error {
		if err := r.client.Del(tid).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "DEL")
	}

	return nil
}

// Find all task ids that contain pattern
func (r *StorageImpl) FindInHeap(ctx context.Context, qid string, pattern string) ([]*types.Task, error) {
	var res []*types.Task
	uniq := make(map[string]bool)
	if err := retry(ctx, func() error {
		var cursor uint64 = 0
		for {
			var err error
			var keys []string
			keys, cursor, err = r.client.Scan(cursor, "", 10).Result()
			if err != nil {
				return newTempError(err)
			}
			if cursor == 0 {
				return nil
			}
			for _, k := range keys {
				taskStr, err := r.client.Get(k).Result()
				if err != nil {
					continue
				}
				task := &types.Task{}
				if err := json.Unmarshal([]byte(taskStr), task); err != nil {
					continue
				}
				body := make(map[string]interface{})
				json.Unmarshal([]byte(task.Body), &body)
				bodyBytes, _ := json.Marshal(body)
				if strings.Contains(string(bodyBytes), pattern) {
					if _, exists := uniq[k]; !exists {
						res = append(res, task)
						uniq[k] = true
					}
				}
			}
		}
	}, defaultRetryLimit); err != nil {
		return nil, errors.Wrap(err, "SCAN heap")
	}

	return res, nil
}

// Put task id to ZSET with score equal to delay
func (r *StorageImpl) PutTaskIdToDelayedTree(ctx context.Context, qid, tid string, delay time.Duration) error {
	if err := retry(ctx, func() error {
		if err := r.client.ZAdd(keyTreeDelayed(qid), &redis.Z{
			Score:  float64(time.Now().Add(delay).Unix()),
			Member: tid,
		}).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "ZADD task id")
	}

	return nil
}

// Put task id to the end of ready list
func (r *StorageImpl) PutTaskIdToReadyList(ctx context.Context, qid, tid string) error {
	if err := retry(ctx, func() error {
		if err := r.client.LPush(keyListReady(qid), tid).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "LPUSH task id")
	}

	return nil
}

// Put task id to buried set
func (r *StorageImpl) PutTaskIdToBuriedSet(ctx context.Context, qid, tid string) error {
	if err := retry(ctx, func() error {
		if err := r.client.SAdd(keySetBuried(qid), tid).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "SADD")
	}

	return nil
}

// Check if buried set contains task id
func (r *StorageImpl) IsTaskBuried(ctx context.Context, qid, tid string) (bool, error) {
	var res bool
	if err := retry(ctx, func() error {
		var err error
		res, err = r.client.SIsMember(keySetBuried(qid), tid).Result()
		if err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return false, errors.Wrap(err, "SISMEMBER")
	}

	return res, nil
}

// Check if delayed tree contains task id
func (r *StorageImpl) IsTaskDelayed(ctx context.Context, qid, tid string) (bool, error) {
	var res bool
	if err := retry(ctx, func() error {
		var err error
		var cursor uint64 = 0
		for {
			var found []string
			found, cursor, err = r.client.ZScan(keyTreeDelayed(qid), cursor, tid, 50).Result()
			if err != nil {
				return newTempError(err)
			}
			if len(found) > 0 {
				res = true
				return nil
			}
			if cursor == 0 {
				break
			}
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return false, errors.Wrap(err, "ZSCAN")
	}

	return res, nil
}

// Check if ready list contains task id. No guarantees for the result if list is being changed
func (r *StorageImpl) IsTaskReady(ctx context.Context, qid, tid string) (bool, error) {
	size, err := r.GetReadyListLen(ctx, qid)
	if err != nil {
		return false, errors.Wrap(err, "get ready list length")
	}

	return r.isTaskInList(ctx, keyListReady(qid), tid, size)
}

// Check if taken list contains task id. No guarantees for the result if list is being changed
func (r *StorageImpl) IsTaskTaken(ctx context.Context, qid, tid string) (bool, error) {
	size, err := r.GetTakenListLen(ctx, qid)
	if err != nil {
		return false, errors.Wrap(err, "get taken list length")
	}

	return r.isTaskInList(ctx, keyListTaken(qid), tid, size)
}

func (r *StorageImpl) isTaskInList(ctx context.Context, listKey, tid string, listSize int64) (bool, error) {
	var res bool
	if err := retry(ctx, func() error {
		const batchSize = 64
		for {
			tids, err := r.client.LRange(listKey, listSize-batchSize, listSize).Result()
			if err != nil {
				return newTempError(err)
			}
			if len(tids) == 0 {
				return nil
			}
			for _, t := range tids {
				if t == tid {
					res = true
					return nil
				}
			}
			listSize -= batchSize
		}
	}, defaultRetryLimit); err != nil {
		return false, errors.Wrap(err, "LRANGE")
	}

	return res, nil
}

// Get random task id from buried set
func (r *StorageImpl) GetRandomBuriedTaskId(ctx context.Context, qid string) (tid string, err error) {
	if err := retry(ctx, func() error {
		var err error
		tid, err = r.client.SRandMember(keySetBuried(qid)).Result()
		if err != nil {
			if err == redis.Nil {
				return ErrorNotFound
			}
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return "", errors.Wrap(err, "SRANDMEMBER")
	}

	return tid, nil
}

// Move next task id from ready list to taken list using BRPOPLPUSH
func (r *StorageImpl) MoveTaskIdFromReadyListToTaken(ctx context.Context, qid string) (taskId string, err error) {
	if err := retry(ctx, func() error {
		var err error
		if taskId, err = r.client.BRPopLPush(keyListReady(qid), keyListTaken(qid), 1*time.Second).Result(); err != nil {
			if err == redis.Nil {
				return ErrorNotFound
			}
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return "", errors.Wrap(err, "BRPOPLPUSH task id")
	}

	return taskId, nil
}

// Drop task id from taken list using LREM
func (r *StorageImpl) DropTaskIdFromTakenList(ctx context.Context, qid, tid string) error {
	if err := retry(ctx, func() error {
		count, err := r.client.LRem(keyListTaken(qid), -1, tid).Result()
		if err != nil {
			return newTempError(err)
		}
		if count != 1 {
			return ErrorNotFound
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "LREM task id")
	}

	return nil
}

// Drop task id from delayed tree using ZREM
func (r *StorageImpl) DropTaskIdFromDelayedTree(ctx context.Context, qid, tid string) error {
	if err := retry(ctx, func() error {
		if err := r.client.ZRem(keyTreeDelayed(qid), tid).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "ZREM task id")
	}

	return nil
}

// Drop task id from buried set using SREM
func (r *StorageImpl) DropTaskIdFromBuriedSet(ctx context.Context, qid, tid string) error {
	if err := retry(ctx, func() error {
		if err := r.client.SRem(keySetBuried(qid), tid).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "SREM task id")
	}

	return nil
}

// Get task id and score (delay timestamp) of the top element in delayed tree
func (r *StorageImpl) GetDelayedHead(ctx context.Context, qid string) (tid string, delayScore float64, err error) {
	var res []redis.Z
	if err := retry(ctx, func() error {
		var err error
		res, err = r.client.ZRangeWithScores(keyTreeDelayed(qid), 0, 0).Result()
		if err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return "", 0, errors.Wrap(err, "ZRANGEWITHSCORES")
	}

	if len(res) == 0 {
		return "", 0, ErrorNotFound
	}

	return res[0].Member.(string), res[0].Score, nil
}

// Get slice of task ids from the beginning of taken list
func (r *StorageImpl) GetTakenSlice(ctx context.Context, qid string, size int64) ([]string, error) {
	return r.getListSlice(ctx, keyListReady(qid), size)
}

// Get slice of top task ids from delayed tree
func (r *StorageImpl) GetDelayedSlice(ctx context.Context, qid string, size int64) ([]string, error) {
	var res []string
	if err := retry(ctx, func() error {
		var err error
		res, err = r.client.ZRange(keyTreeDelayed(qid), 0, size).Result()
		if err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return nil, errors.Wrap(err, "ZRANGE")
	}

	return res, nil
}

// Get slice of task ids from beginning of ready slice
func (r *StorageImpl) GetReadySlice(ctx context.Context, qid string, size int64) ([]string, error) {
	return r.getListSlice(ctx, keyListReady(qid), size)
}

// Get slice of random elements from buried set
func (r *StorageImpl) GetBuriedSlice(ctx context.Context, qid string, size int64) ([]string, error) {
	var res []string
	if err := retry(ctx, func() error {
		var err error
		res, err = r.client.SRandMemberN(keySetBuried(qid), size).Result()
		if err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return nil, errors.Wrap(err, "SRANDMEMBER")
	}

	return res, nil
}

// Increment value of done counter using INCR
func (r *StorageImpl) IncrementDoneCounter(ctx context.Context, qid string) error {
	if err := retry(ctx, func() error {
		if err := r.client.Incr(keyCounterDone(qid)).Err(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return errors.Wrap(err, "INCR")
	}

	return nil
}

// Get size of ready list using LLEN
func (r *StorageImpl) GetReadyListLen(ctx context.Context, qid string) (int64, error) {
	return r.getListLen(ctx, keyListReady(qid))
}

// Get size of taken list using LLEN
func (r *StorageImpl) GetTakenListLen(ctx context.Context, qid string) (int64, error) {
	return r.getListLen(ctx, keyListTaken(qid))
}

// Get size of delayed tree using ZCOUNT
func (r *StorageImpl) GetDelayedTreeSize(ctx context.Context, qid string) (int64, error) {
	var res int64
	if err := retry(ctx, func() error {
		var err error
		if res, err = r.client.ZCount(keyTreeDelayed(qid), "-inf", "+inf").Result(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return -1, errors.Wrap(err, "ZCOUNT")
	}

	return res, nil
}

// Get buried set cardinality
func (r *StorageImpl) GetBuriedSetSize(ctx context.Context, qid string) (int64, error) {
	var res int64
	if err := retry(ctx, func() error {
		var err error
		res, err = r.client.SCard(keySetBuried(qid)).Result()
		if err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return -1, errors.Wrap(err, "SCARD")
	}

	return res, nil
}

// Get value of done counter
func (r *StorageImpl) GetDoneCounterValue(ctx context.Context, qid string) (int64, error) {
	var res int64 = 0
	if err := retry(ctx, func() error {
		doneStr, err := r.client.Get(keyCounterDone(qid)).Result()
		if err != nil {
			if err != redis.Nil {
				return newTempError(err)
			}
			return nil
		}
		res, err = strconv.ParseInt(doneStr, 10, 64)
		if err != nil {
			return errors.Wrap(err, "parse result string")
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return -1, errors.Wrap(err, "GET")
	}

	return res, nil
}

// Try lock before validating taken
func (r *StorageImpl) TryLockBeforeValidatingTaken(ctx context.Context, qid string, period time.Duration) (func() error, error) {
	return r.tryLock(ctx, lockTakenValidation(qid), period)
}

// Try lock before traversing delayed
func (r *StorageImpl) TryLockBeforeTraversingDelayed(ctx context.Context, qid string, period time.Duration) (func() error, error) {
	return r.tryLock(ctx, lockTreeDelayed(qid), period)
}

// Try lock before kicking all task ids
func (r *StorageImpl) TryLockBeforeKick(ctx context.Context, qid string, period time.Duration) (func() error, error) {
	return r.tryLock(ctx, lockKickAllInProgress(qid), period)
}

func (r *StorageImpl) tryLock(ctx context.Context, key string, period time.Duration) (func() error, error) {
	var releaseFunc func() error

	if err := retry(ctx, func() error {
		lock, err := r.locker.Obtain(key, period, nil)
		if err != nil {
			if err == redislock.ErrNotObtained {
				// If lock already obtained
				return ErrorLocked
			}
			return newTempError(err)
		}
		releaseFunc = func() error {
			return retry(ctx, func() error {
				if err := lock.Release(); err != nil {
					return newTempError(err)
				}
				return nil
			}, defaultRetryLimit)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return nil, errors.Wrap(err, "obtain lock")
	}

	return releaseFunc, nil
}

func (r *StorageImpl) getListSlice(ctx context.Context, listKey string, size int64) ([]string, error) {
	var res []string
	if err := retry(ctx, func() error {
		var err error
		res, err = r.client.LRange(listKey, 0, size).Result()
		if err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return nil, errors.Wrap(err, "LRANGE")
	}

	return res, nil
}

func (r *StorageImpl) getListLen(ctx context.Context, key string) (int64, error) {
	var res int64
	if err := retry(ctx, func() error {
		var err error
		if res, err = r.client.LLen(key).Result(); err != nil {
			return newTempError(err)
		}
		return nil
	}, defaultRetryLimit); err != nil {
		return -1, errors.Wrap(err, "LLEN")
	}

	return res, nil
}
