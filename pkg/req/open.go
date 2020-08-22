package req

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/kilchik/req/internal/pkg/storage"
	"github.com/kilchik/req/pkg/logger"
	"github.com/pkg/errors"
)

const (
	defaultTakenValidationPeriod = 15 * time.Minute
	defaultTakeTimeout           = 30 * time.Minute
)

func Open(ctx context.Context, store storage.Storage, logger logger.Logger, options ...func(q *Q) error) (*Q, error) {
	q := &Q{
		store:                 store,
		logger:                logger,
		takeTimeout:           defaultTakeTimeout,
		takenValidationPeriod: defaultTakenValidationPeriod,
	}

	for _, option := range options {
		if err := option(q); err != nil {
			return nil, errors.Wrap(err, "apply option")
		}
	}

	// Map id to name if present
	if q.name == "" {
		q.name = "default"
	}
	qid, err := store.GetQId(ctx, q.name)
	if err == storage.ErrorNotFound {
		q.id = generateQID()
		if err := q.store.SetQIdForName(ctx, q.id, q.name); err != nil {
			return nil, errors.Wrapf(err, "bind queue id to name")
		}
	} else {
		q.id = qid
	}

	go q.traverseDelayed(ctx)
	go q.validateTaken(ctx, q.takenValidationPeriod)

	return q, nil
}

func generateQID() string {
	return fmt.Sprintf("%d%d", time.Now().Unix(), 100+rand.Intn(900))
}
