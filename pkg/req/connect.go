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

func DisableLogger(f *Fabriq) error {
	f.logger = &defaultLogger{disabled: true}
	return nil
}

func SetLogger(logger Logger) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		f.logger = logger
		return nil
	}
}

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

func UseSentinel(masterName, password string, sentinelAddrs []string) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		if f.connType == connTypeRedis {
			return errors.New("you can use either sentinel option or redis option")
		}
		f.client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       masterName,
			SentinelAddrs:    sentinelAddrs,
			SentinelPassword: password,
		})
		return nil
	}
}

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

func MustConnect(ctx context.Context, options ...func(q *Fabriq) error) *Fabriq {
	f, err := Connect(ctx, options...)
	if err != nil {
		panic(err)
	}
	return f
}
