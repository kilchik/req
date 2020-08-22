package fabriq

import (
	"errors"

	"github.com/go-redis/redis/v7"
	"github.com/kilchik/req/pkg/logger"
)

// DisableLogger disables logging
func DisableLogger(f *Fabriq) error {
	logger := &logger.DefaultLogger{}
	logger.Disable()
	f.logger = logger
	return nil
}

// SetLogger lets you specify custom logger
func SetLogger(logger logger.Logger) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		f.logger = logger
		return nil
	}
}

// SetRedis lets you specify storage address and password (you can use this option or UseSentinel - not both)
func SetRedis(addr, password string) func(f *Fabriq) error {
	return func(f *Fabriq) error {
		if f.connType == connTypeSentinel {
			return errors.New("you can use either sentinel option or storage option")
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
			return errors.New("you can use either sentinel option or storage option")
		}
		f.client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			SentinelAddrs: sentinelAddrs,
			Password:      password,
		})
		return nil
	}
}
