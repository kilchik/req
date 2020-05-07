package req

import (
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
)

const (
	defaultTakenValidationPeriod = 15 * time.Minute
	defaultTakeTimeout           = 30 * time.Minute
)

type Q struct {
	id     string
	name   string
	client *redis.Client
	locker *redislock.Client
	logger Logger

	takeTimeout           time.Duration
	takenValidationPeriod time.Duration
}

func (q *Q) GetId() string {
	return q.id
}
