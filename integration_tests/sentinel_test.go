// +build sentinel

package integration_tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/kilchik/req/pkg/fabriq"
	"github.com/kilchik/req/pkg/req"
	"github.com/kilchik/req/pkg/types"
	"github.com/stretchr/testify/suite"
)

type SentinelTestSuite struct {
	suite.Suite
	fabriq *fabriq.Fabriq
	q      *req.Q
	redis  *redis.Client
}

func (suite *SentinelTestSuite) SetupTest() {
	suite.fabriq = fabriq.MustConnect(context.Background(), fabriq.UseSentinel("mymaster", "", []string{"localhost:50000"}))
	suite.q = suite.fabriq.MustOpen(context.Background())
	suite.redis = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	suite.redis.FlushAll()
}

func (suite *SentinelTestSuite) TestCommands() {
	suite.Run("put", func() {
		taskId, err := suite.q.Put(context.Background(), "abc", 0)
		suite.Require().Nil(err)

		taskStr, err := suite.redis.Get(taskId).Result()
		suite.Require().Nil(err)

		t := &types.Task{}
		err = json.Unmarshal([]byte(taskStr), t)
		suite.Require().Nil(err)
		suite.Equal(taskId, t.Id)
		suite.EqualValues(0, t.Delay)
		suite.Equal([]byte("\"abc\""), t.Body) // "abc" since json serialized

		res, err := suite.redis.LRange("req_list_ready"+suite.q.GetId(), 0, 1).Result()
		suite.Require().Nil(err)
		suite.Require().Len(res, 1)
		suite.Equal(taskId, res[0])

		taskId, err = suite.q.Put(context.Background(), "abc", 0)
		suite.Require().Nil(err)
		res, err = suite.redis.LRange("req_list_ready"+suite.q.GetId(), 0, 2).Result()
		suite.Require().Nil(err)
		suite.Require().Len(res, 2)
		suite.Equal(taskId, res[0])
	})

	suite.redis.FlushAll()

	suite.Run("take", func() {
		payload, _ := json.Marshal("abc")
		t, _ := json.Marshal(&types.Task{
			Id:    "task_uuid",
			Delay: 0,
			Body:  payload,
		})
		suite.Require().Nil(suite.redis.Set("task_uuid", t, 0).Err())
		suite.Require().Nil(suite.redis.LPush("req_list_ready"+suite.q.GetId(), "task_uuid").Err())

		var dst string
		id, err := suite.q.Take(context.Background(), &dst)
		suite.Require().Nil(err)
		suite.Equal("task_uuid", id)
		suite.Equal("abc", dst)

		ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
		id, err = suite.q.Take(ctx, &dst)
		suite.Equal("", id)
		suite.Equal(context.Canceled, err)
	})

	suite.redis.FlushAll()

	suite.Run("ack", func() {
		_, err := suite.q.Put(context.Background(), "abc", 0)
		suite.Require().Nil(err)

		var dst string
		taskId, err := suite.q.Take(context.Background(), &dst)
		suite.Require().Nil(err)
		resArr, err := suite.redis.LRange("req_list_taken"+suite.q.GetId(), 0, 1).Result()
		suite.Require().Nil(err)
		suite.Len(resArr, 1)

		err = suite.q.Ack(context.Background(), taskId)
		suite.Require().Nil(err)
		res, err := suite.redis.Get(taskId).Result()
		suite.Equal(redis.Nil, err)
		suite.Equal("", res)

		resArr, err = suite.redis.LRange("req_list_taken"+suite.q.GetId(), 0, 0).Result()
		suite.Require().Nil(err)
		suite.Empty(resArr)
	})
}

func TestSentinelTestSuite(t *testing.T) {
	suite.Run(t, new(SentinelTestSuite))
}
