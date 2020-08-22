package fabriq

import (
	"context"
	"math/rand"
	"time"

	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v7"
	"github.com/kilchik/req/internal/pkg/storage"
	"github.com/kilchik/req/pkg/logger"
	"github.com/kilchik/req/pkg/req"
	"github.com/pkg/errors"
)

type Fabriq struct {
	client   *redis.Client
	locker   *redislock.Client
	logger   logger.Logger
	connType int
}

func (f *Fabriq) Open(ctx context.Context, options ...func(q *req.Q) error) (*req.Q, error) {
	store := storage.New(f.client, f.locker, f.logger)
	return req.Open(ctx, store, f.logger, options...)
}

func (f *Fabriq) MustOpen(ctx context.Context, options ...func(q *req.Q) error) *req.Q {
	q, err := f.Open(ctx, options...)
	if err != nil {
		panic(err)
	}
	return q
}

func (f *Fabriq) OpenWithHandler(ctx context.Context, task interface{}, handler req.HandlerFunc, options ...func(q *req.Q) error) (*req.AsynQ, error) {
	q, err := f.Open(ctx, options...)
	if err != nil {
		return nil, errors.Wrap(err, "create queue")
	}
	return req.NewAsynQ(ctx, q, task, handler), nil
}

func (f *Fabriq) MustOpenWithHandler(ctx context.Context, task interface{}, handler req.HandlerFunc, options ...func(q *req.Q) error) *req.AsynQ {
	aq, err := f.OpenWithHandler(ctx, task, handler)
	if err != nil {
		panic(err)
	}
	return aq
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
