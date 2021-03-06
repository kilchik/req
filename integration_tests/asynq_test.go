package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/kilchik/req/pkg/fabriq"
	"github.com/stretchr/testify/suite"
)

type AsynqTestSuite struct {
	suite.Suite
	fabriq *fabriq.Fabriq
	redis  *redis.Client
}

func (suite *AsynqTestSuite) SetupTest() {
	suite.fabriq = fabriq.MustConnect(context.Background())
	suite.redis = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	suite.redis.FlushAll()
}

func (suite *AsynqTestSuite) TestHallelujahWithAsyncHandler() {
	type concatTask struct {
		NewLetter string
	}
	var res string
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	aq := suite.fabriq.MustOpenWithHandler(ctx, &concatTask{}, func(ctx context.Context, taskId string, task interface{}) error {
		res += task.(*concatTask).NewLetter
		return nil
	})

	for _, l := range "Hallelujah" {
		err := aq.Put(ctx, &concatTask{NewLetter: string(l)}, 0)
		suite.Require().Nil(err)
	}
	time.Sleep(500 * time.Millisecond)
	cancel()
	time.Sleep(500 * time.Millisecond)
	suite.Equal("Hallelujah", res)
	time.Sleep(1 * time.Second)
}

func TestAsynqTestSuite(t *testing.T) {
	suite.Run(t, new(AsynqTestSuite))
}
