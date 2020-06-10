package req

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

type HandlerFunc func(ctx context.Context, taskId string, task interface{}) error

type errorTempWithCustomDelay struct {
	from, to time.Duration
}

func (e errorTempWithCustomDelay) Error() string {
	return "custom temp error"
}

var (
	errorFatal   = errors.New("fatal error")
	errorTemp    = errors.New("temp error")
	errorUnknown = errors.New("unknown error")
)

func NewErrorFatal(format string, args ...interface{}) error {
	return errors.Wrapf(errorFatal, format, args...)
}

func NewErrorTemp(format string, args ...interface{}) error {
	return errors.Wrapf(errorTemp, format, args...)
}

func NewErrorTempWithCustomDelay(delayFrom, delayTo time.Duration, format string, args ...interface{}) error {
	return errors.Wrapf(&errorTempWithCustomDelay{delayFrom, delayTo}, format, args...)
}

func NewErrorUnknown(format string, args ...interface{}) error {
	return errors.Wrapf(errorUnknown, format, args...)
}

type AsynQ struct {
	*Q
	handler HandlerFunc
}

// NewAsynQ creates and runs new queue using handler as a function for processing taken tasks
func NewAsynQ(ctx context.Context, q *Q, task interface{}, handler HandlerFunc) *AsynQ {
	delay := timeoutExp()
	go func() {
		for {
			select {
			case <-ctx.Done():
				q.logger.Infof(ctx, "stopped on context signal")
				return
			default:
				taskId, err := q.Take(ctx, &task)
				if err != nil {
					q.logger.Errorf(ctx, "take next task: %v", err)
					time.Sleep(delay())
					continue
				}
				if err = handler(ctx, taskId, task); err != nil {
					switch errors.Cause(err) {
					case errorFatal:
						q.logger.Infof(ctx, "task handler failed with fatal error: %v; deleting task %q", err, taskId)
						if err := q.Delete(ctx, taskId); err != nil {
							q.logger.Errorf(ctx, "delete task: %v", err)
						}
					case errorUnknown:
						q.logger.Infof(ctx, "task handler failed with unknown error: %v; burying task %q", err, taskId)
						if err := q.Bury(ctx, taskId); err != nil {
							q.logger.Errorf(ctx, "bury task: %v", err)
						}
					case errorTemp:
						q.logger.Infof(ctx, "task handler failed with temp error: %v; delaying task %q", err, taskId)
						if err := q.Delay(ctx, taskId); err != nil {
							q.logger.Errorf(ctx, "delay task: %v", err)
						}
					default:
						if errTmpCustom, ok := errors.Cause(err).(*errorTempWithCustomDelay); ok {
							q.logger.Infof(ctx, "task handler failed with custom temp error: %v; delaying task %q", err, taskId)
							if err := q.DelayCustom(ctx, taskId, errTmpCustom.from, errTmpCustom.to); err != nil {
								q.logger.Errorf(ctx, "delay task: %v", err)
							}
						} else {
							q.logger.Errorf(ctx, "task handler failed with unexpected error: %v; delaying task %q", err, taskId)
							if err := q.Delay(ctx, taskId); err != nil {
								q.logger.Errorf(ctx, "delay task: %v", err)
							}
						}
					}
					continue
				}
				if err := q.Ack(ctx, taskId); err != nil {
					q.logger.Errorf(ctx, "ack task: %v", err)
				}
			}
		}
	}()
	return &AsynQ{q, handler}
}

func (aq *AsynQ) Put(ctx context.Context, task interface{}, delay time.Duration) error {
	_, err := aq.Q.Put(ctx, task, delay)
	return err
}
