package req

import (
	"time"

	"github.com/kilchik/req/internal/pkg/storage"
	"github.com/kilchik/req/pkg/logger"
)

type Q struct {
	id   string
	name string

	store  storage.Storage
	logger logger.Logger

	takeTimeout           time.Duration
	takenValidationPeriod time.Duration
}

func (q *Q) GetId() string {
	return q.id
}
