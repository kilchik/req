package req

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/suite"
)

type ReqOpsTestSuite struct {
	suite.Suite
	q     *Q
	redis *redis.Client
}

func (suite *ReqOpsTestSuite) SetupTest() {
	suite.q = MustConnect(context.Background(), "localhost:6379", "", DisableLogger)
	suite.redis = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	suite.redis.FlushAll()
}

func (suite *ReqOpsTestSuite) TestPutWithZeroDelay() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	taskStr, err := suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)

	t := &Task{}
	err = json.Unmarshal([]byte(taskStr), t)
	suite.Require().Nil(err)
	suite.Equal(taskId, t.Id)
	suite.EqualValues(0, t.Delay)
	suite.Equal([]byte("\"abc\""), t.Body) // "abc" since json serialized

	res, err := suite.redis.LRange("req_list_ready", 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 1)
	suite.Equal(taskId, res[0])

	taskId, err = suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)
	res, err = suite.redis.LRange("req_list_ready", 0, 2).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 2)
	suite.Equal(taskId, res[0])
}

func (suite *ReqOpsTestSuite) TestPutWithNonZeroDelay() {
	taskId, err := suite.q.Put(context.Background(), "abc", 2*time.Second)
	suite.Require().Nil(err)

	taskStr, err := suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)
	suite.Greater(len(taskStr), 3)

	res, err := suite.redis.LRange("req_list_ready", 0, 0).Result()
	suite.Require().Nil(err)
	suite.Empty(res)

	res, err = suite.redis.ZRange("req_tree_delayed", 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 1)
	suite.Equal(taskId, res[0])

	// In 2 seconds the task must transfer from 'delayed' tree to 'ready' list
	time.Sleep(2*time.Second + 100*time.Millisecond)

	res, err = suite.redis.LRange("req_list_ready", 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 1)
	suite.Equal(taskId, res[0])

	res, err = suite.redis.ZRange("req_tree_delayed", 0, 0).Result()
	suite.Require().Nil(err)
	suite.Empty(res)
}

func (suite *ReqOpsTestSuite) TestTake() {
	payload, _ := json.Marshal("abc")
	t, _ := json.Marshal(&Task{
		Id:    "task_uuid",
		Delay: 0,
		Body:  payload,
	})
	suite.Require().Nil(suite.redis.Set("task_uuid", t, 0).Err())
	suite.Require().Nil(suite.redis.LPush("req_list_ready", "task_uuid").Err())

	var dst string
	id, err := suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	suite.Equal("task_uuid", id)
	suite.Equal("abc", dst)

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	id, err = suite.q.Take(ctx, &dst)
	suite.Equal("", id)
	suite.Equal(context.Canceled, err)
}

func (suite *ReqOpsTestSuite) TestAck() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	var dst string
	taskId, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	resArr, err := suite.redis.LRange("req_list_taken", 0, 1).Result()
	suite.Require().Nil(err)
	suite.Len(resArr, 1)

	err = suite.q.Ack(context.Background(), taskId)
	suite.Require().Nil(err)
	res, err := suite.redis.Get(taskId).Result()
	suite.Equal(redis.Nil, err)
	suite.Equal("", res)

	resArr, err = suite.redis.LRange("req_list_taken", 0, 0).Result()
	suite.Require().Nil(err)
	suite.Empty(resArr)
}

func TestReqOpsTestSuite(t *testing.T) {
	suite.Run(t, new(ReqOpsTestSuite))
}
