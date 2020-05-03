package req

import (
	"context"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

const (
	defaultTakenValidationPeriod = 15 * time.Minute
	defaultTakeTimeout           = 30 * time.Minute
)

func DisableLogger(q *Q) error {
	q.logger = &defaultLogger{disabled: true}
	return nil
}

func SetLogger(logger Logger) func(q *Q) error {
	return func(q *Q) error {
		q.logger = logger
		return nil
	}
}

func SetTakeTimeout(timeout time.Duration) func(q *Q) error {
	return func(q *Q) error {
		q.takeTimeout = timeout
		return nil
	}
}

func SetTakenValidationPeriod(period time.Duration) func(q *Q) error {
	return func(q *Q) error {
		q.takenValidationPeriod = period
		return nil
	}
}

func Connect(ctx context.Context, redisAddr, redisPasswd string, options ...func(q *Q) error) (*Q, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPasswd,
		DB:       0,
	})

	status := client.Ping()
	if status.Err() != nil {
		return nil, errors.Wrap(status.Err(), "ping redis")
	}

	q := &Q{
		client: client,
		locker: redislock.New(client),
	}

	// Set up defaults
	q.logger = &defaultLogger{}
	q.takenValidationPeriod = defaultTakenValidationPeriod
	q.takeTimeout = defaultTakeTimeout

	for _, option := range options {
		if err := option(q); err != nil {
			return nil, errors.Wrap(err, "apply option")
		}
	}

	go q.traverseDelayed(ctx)
	go q.validateTaken(ctx, q.takenValidationPeriod)

	return q, nil
}

func MustConnect(ctx context.Context, redisAddr, redisPasswd string, options ...func(q *Q) error) *Q {
	q, err := Connect(ctx, redisAddr, redisPasswd, options...)
	if err != nil {
		panic(err)
	}
	return q
}
