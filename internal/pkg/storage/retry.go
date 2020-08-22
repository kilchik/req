package storage

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

const startSleep = 50 * time.Millisecond

type tempError struct {
	err error
}

func newTempError(err error) *tempError {
	return &tempError{err}
}

func (e tempError) Error() string {
	return e.err.Error()
}

func retry(ctx context.Context, f func() error, times int) error {
	var err error
	for t := 1; t <= times; t += 1 {
		select {
		case <-ctx.Done():
			return err
		default:
			if err = f(); err != nil {
				if _, ok := errors.Cause(err).(*tempError); ok {
					time.Sleep(time.Duration(t) * startSleep)
					continue
				}
				return err
			}
			return nil
		}
	}
	return err
}
