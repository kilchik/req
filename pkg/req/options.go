package req

import "time"

func SetName(name string) func(q *Q) error {
	return func(q *Q) error {
		q.name = name
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
