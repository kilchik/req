package req

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/suite"
)

type AsynqTestSuite struct {
	suite.Suite
	fabriq      *Fabriq
	redis  *redis.Client
}

func (suite *AsynqTestSuite) SetupTest() {
	suite.fabriq = MustConnect(context.Background(), DisableLogger)
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
	aq := suite.fabriq.MustCreateWithHandler(context.Background(), &concatTask{}, func(ctx context.Context, taskId string, task interface{}) error {
		res += task.(*concatTask).NewLetter
		return nil
	})

	for _, l := range "Hallelujah" {
		err := aq.Put(context.Background(), &concatTask{NewLetter:string(l)}, 0)
		suite.Require().Nil(err)
	}
	time.Sleep(2*time.Second)
	suite.Equal("Hallelujah", res)
}

func TestAsynqTestSuite(t *testing.T) {
	suite.Run(t, new(AsynqTestSuite))
}

