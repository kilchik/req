package req

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/suite"
)

type ValidateTakenTestSuite struct {
	suite.Suite
	q     *Q
	redis *redis.Client
}

func (suite *ValidateTakenTestSuite) SetupTest() {
	suite.q = MustConnect(context.Background()).
		MustCreate(context.Background(), SetTakeTimeout(1*time.Second), SetTakenValidationPeriod(1*time.Second))
	suite.redis = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	suite.redis.FlushAll()
}

func (suite *ValidateTakenTestSuite) TestTraverseTaken() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)
	var res string
	_, err = suite.q.Take(context.Background(), &res)
	suite.Require().Nil(err)

	// Check that at first there is a single task in taken list and there are no tasks in ready list
	resArr, err := suite.redis.LRange("req_list_taken"+suite.q.GetId(), 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(resArr, 1)
	suite.Require().Equal(taskId, resArr[0])
	resArr, err = suite.redis.LRange("req_list_ready"+suite.q.GetId(), 0, 0).Result()
	suite.Require().Nil(err)
	suite.Require().Empty(resArr)

	time.Sleep(3 * time.Second)

	// Check that after 3 seconds the single task moved from taken list to ready list due to take timeout expiration
	resArr, err = suite.redis.LRange("req_list_taken"+suite.q.GetId(), 0, 0).Result()
	suite.Require().Nil(err)
	suite.Require().Empty(resArr)
	resArr, err = suite.redis.LRange("req_list_ready"+suite.q.GetId(), 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(resArr, 1)
	suite.Require().Equal(taskId, resArr[0])
}

func TestValidateTakenTestSuite(t *testing.T) {
	suite.Run(t, new(ValidateTakenTestSuite))
}
