package req

import (
	"context"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
)

const (
	connTypeLocal    = iota
	connTypeRedis    = iota
	connTypeSentinel = iota
)

// DisableLogger disables logging
func DisableLogger(f *Fabriq) error {
	f.logger = &defaultLogger{disabled: true}
	return nil
}

// SetLogger lets you specify custom logger
func SetLogger(logger Logger) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		f.logger = logger
		return nil
	}
}

// SetRedis lets you specify redis address and password (you can use this option or UseSentinel - not both)
func SetRedis(addr, password string) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		if f.connType == connTypeSentinel {
			return errors.New("you can use either sentinel option or redis option")
		}
		f.client = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
		})
		return nil
	}
}

// UseSentinel let you specify sentinel failover options (you can use this option or SetRedis - not both)
func UseSentinel(masterName, password string, sentinelAddrs []string) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		if f.connType == connTypeRedis {
			return errors.New("you can use either sentinel option or redis option")
		}
		f.client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: sentinelAddrs,
			Password:      password,
		})
		return nil
	}
}

// Connect creates Fabriq object and connects to redis using listed options; Fabriq object is used for creating queues
// within single redis
func Connect(ctx context.Context, options ...func(f *Fabriq) error) (*Fabriq, error) {
	f := &Fabriq{}

	// Set up defaults
	f.client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	f.locker = redislock.New(f.client)
	f.logger = &defaultLogger{}
	f.connType = connTypeLocal

	for _, option := range options {
		if err := option(f); err != nil {
			return nil, errors.Wrap(err, "apply option")
		}
	}

	if err := f.client.Ping().Err(); err != nil {
		return nil, errors.Wrap(err, "ping redis")
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
