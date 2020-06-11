package req

import (
	"context"
	"testing"

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
	suite.fabriq = MustConnect(context.Background(), DisableLogger)
	suite.q = suite.fabriq.MustCreate(context.Background(), SetName("myqueue"))
	suite.redis = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	suite.redis.FlushAll()
}

func (suite *ConnectTestSuite) TestReconnectToExistingQueue() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	qSame := suite.fabriq.MustCreate(context.Background(), SetName("myqueue"))
	var taken string
	taskId2, err := qSame.Take(context.Background(), &taken)
	suite.Require().Nil(err)
	suite.Require().EqualValues(taskId, taskId2)
	suite.Require().EqualValues("abc", taken)
}

func TestConnectTestSuite(t *testing.T) {
	suite.Run(t, new(ConnectTestSuite))
}
