package req

import "time"

func timeoutExp() func() time.Duration {
	timeout := time.Second
	return func() time.Duration {
		timeout *= 2
		return timeout
	}
}
