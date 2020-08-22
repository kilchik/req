package fabriq

import (
	"context"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/kilchik/req/pkg/logger"
	"github.com/pkg/errors"
)

const (
	connTypeLocal    = iota
	connTypeRedis    = iota
	connTypeSentinel = iota
)

// Connect creates Fabriq object and connects to storage using listed options; Fabriq object is used for creating queues
// within single storage
func Connect(ctx context.Context, options ...func(f *Fabriq) error) (*Fabriq, error) {
	f := &Fabriq{}

	// Set up defaults
	f.client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	f.locker = redislock.New(f.client)
	f.logger = &logger.DefaultLogger{}
	f.connType = connTypeLocal

	for _, option := range options {
		if err := option(f); err != nil {
			return nil, errors.Wrap(err, "apply option")
		}
	}

	if err := f.client.Ping().Err(); err != nil {
		return nil, errors.Wrap(err, "ping storage")
	}

	return f, nil
}

// MustConnect runs Connect and panics if failed
func MustConnect(ctx context.Context, options ...func(q *Fabriq) error) *Fabriq {
	f, err := Connect(ctx, options...)
	if err != nil {
		panic(err)
	}
	return f
}
