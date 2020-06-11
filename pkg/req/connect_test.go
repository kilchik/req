package req

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/suite"
)

type ConnectTestSuite struct {
	suite.Suite
	fabriq *Fabriq
	q      *Q
	redis  *redis.Client
}

func (suite *ConnectTestSuite) SetupTest() {
	suite.redis = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	suite.redis.FlushAll()
	suite.fabriq = MustConnect(context.Background(), DisableLogger)
	suite.q = suite.fabriq.MustOpen(context.Background(), SetName("myqueue"))
}

func (suite *ConnectTestSuite) TestReconnectToExistingQueue() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	fabriq := MustConnect(context.Background(), DisableLogger)
	qSame := fabriq.MustOpen(context.Background(), SetName("myqueue"))

	qid, err := suite.redis.Get("myqueue").Result()
	suite.Require().Nil(err)
	suite.EqualValues(qid, qSame.GetId())
	suite.EqualValues(qid, suite.q.GetId())

	var taken string
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	taskId2, err := qSame.Take(ctx, &taken)
	suite.Require().Nil(err)
	suite.Require().EqualValues(taskId, taskId2)
	suite.Require().EqualValues("abc", taken)
}

func TestConnectTestSuite(t *testing.T) {
	suite.Run(t, new(ConnectTestSuite))
}
