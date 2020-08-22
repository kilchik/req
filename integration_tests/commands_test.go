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

type ReqOpsTestSuite struct {
	suite.Suite
	fabriq *fabriq.Fabriq
	q      *req.Q
	redis  *redis.Client
}

func (suite *ReqOpsTestSuite) SetupTest() {
	suite.fabriq = fabriq.MustConnect(context.Background(), fabriq.DisableLogger)
	suite.q = suite.fabriq.MustOpen(context.Background())
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
}

func (suite *ReqOpsTestSuite) TestPutWithNonZeroDelay() {
	taskId, err := suite.q.Put(context.Background(), "abc", 2*time.Second)
	suite.Require().Nil(err)

	taskStr, err := suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)
	suite.Greater(len(taskStr), 3)

	res, err := suite.redis.LRange("req_list_ready"+suite.q.GetId(), 0, 0).Result()
	suite.Require().Nil(err)
	suite.Empty(res)

	res, err = suite.redis.ZRange("req_tree_delayed"+suite.q.GetId(), 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 1)
	suite.Equal(taskId, res[0])

	// In 2 seconds the task must transfer from 'delayed' tree to 'ready' list
	time.Sleep(2*time.Second + 100*time.Millisecond)

	res, err = suite.redis.LRange("req_list_ready"+suite.q.GetId(), 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 1)
	suite.Equal(taskId, res[0])

	res, err = suite.redis.ZRange("req_tree_delayed"+suite.q.GetId(), 0, 0).Result()
	suite.Require().Nil(err)
	suite.Empty(res)
}

func (suite *ReqOpsTestSuite) TestTake() {
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
}

func (suite *ReqOpsTestSuite) TestAck() {
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
}

func (suite *ReqOpsTestSuite) TestStat() {
	_, err := suite.q.Put(context.Background(), "abc", 5*time.Second)
	suite.Require().Nil(err)
	_, err = suite.q.Put(context.Background(), "def", 0)
	suite.Require().Nil(err)
	_, err = suite.q.Put(context.Background(), "ghi", 0)
	suite.Require().Nil(err)
	_, err = suite.q.Put(context.Background(), "jkl", 0)
	suite.Require().Nil(err)
	var dst string
	_, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	var taskId string
	taskId, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	err = suite.q.Ack(context.Background(), taskId)
	suite.Require().Nil(err)

	stat, err := suite.q.Stat(context.Background())
	suite.Require().Nil(err)
	suite.EqualValues(1, stat.Ready)
	suite.EqualValues(1, stat.Taken)
	suite.EqualValues(1, stat.Delayed)
	suite.EqualValues(1, stat.Done)
}

func (suite *ReqOpsTestSuite) TestDelete() {
	_, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)
	var dst string
	taskId, err := suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)

	err = suite.q.Delete(context.Background(), taskId)
	suite.Require().Nil(err)
	err = suite.q.Delete(context.Background(), taskId)
	suite.Require().NotNil(err)
}

func (suite *ReqOpsTestSuite) TestMultipleQueues() {
	q2, err := suite.fabriq.Open(context.Background(), req.SetName("yet another queue"))
	suite.Require().Nil(err)

	taskIdQ1, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	taskIdQ2, err := q2.Put(context.Background(), "def", 0)
	suite.Require().Nil(err)

	var dst string
	tid, err := suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	suite.Equal(taskIdQ1, tid)
	suite.Equal("abc", dst)

	tid, err = q2.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	suite.Equal(taskIdQ2, tid)
	suite.Equal("def", dst)
}

func (suite *ReqOpsTestSuite) TestDelayTask() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	var dst string
	_, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)

	err = suite.q.Delay(context.Background(), taskId)
	suite.Require().Nil(err)

	// Check that the task id is not left in taken list
	takenCnt, err := suite.redis.LLen("req_list_taken" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(0, takenCnt)

	// Check that the task id is present in delayed tree
	res, err := suite.redis.ZRange("req_tree_delayed"+suite.q.GetId(), 0, 1).Result()
	suite.Require().Nil(err)
	suite.Require().Len(res, 1)
	suite.Equal(taskId, res[0])

	// Check that task in heap has delay equal to 1 second
	taskStr, err := suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)
	t := &types.Task{}
	suite.Require().Nil(json.Unmarshal([]byte(taskStr), t))
	suite.Equal(1*time.Second, t.Delay)

	_, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	started := time.Now()
	err = suite.q.Delay(context.Background(), taskId)
	suite.Require().Nil(err)

	_, err = suite.q.Take(context.Background(), taskId)
	suite.Require().Nil(err)

	// Check that elapsed time is [1.5, 2.5]s of delay + [0, 1]s of delayed tree traversal
	elapsed := time.Now().Sub(started)
	suite.True(time.Now().After(started.Add(3*time.Second/2)), "elapsed: %v", elapsed)
	suite.True(time.Now().Before(started.Add(7*time.Second/2 + 100*time.Millisecond)))

	err = suite.q.Delay(context.Background(), taskId)
	suite.Require().Nil(err)

	// Check that task in heap has new delay between [1.5, 2.5) * prev delay + [0, 1]s of delayed tree traversal
	taskStr, err = suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)
	t = &types.Task{}
	suite.Require().Nil(json.Unmarshal([]byte(taskStr), t))
	suite.True(t.Delay >= 3*elapsed/2 && t.Delay < 7*elapsed/2,
		"delay value: %v (expected %v <= delay < %v)", t.Delay, 3*elapsed/2, t.Delay < 7*elapsed/2)
}

func (suite *ReqOpsTestSuite) TestDelayCustom() {
	taskId, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	var dst string
	_, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)

	err = suite.q.DelayCustom(context.Background(), taskId, 2*time.Second, 3*time.Second)
	suite.Require().Nil(err)

	taskStr, err := suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)
	t := &types.Task{}
	suite.Require().Nil(json.Unmarshal([]byte(taskStr), t))
	suite.EqualValues(2*time.Second, t.Delay)

	_, err = suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)

	err = suite.q.DelayCustom(context.Background(), taskId, 2*time.Second, 3*time.Second)
	suite.Require().Nil(err)

	taskStr, err = suite.redis.Get(taskId).Result()
	suite.Require().Nil(err)
	t = &types.Task{}
	suite.Require().Nil(json.Unmarshal([]byte(taskStr), t))
	suite.EqualValues(3*time.Second, t.Delay)
}

func (suite *ReqOpsTestSuite) TestBury() {
	_, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	var dst string
	taskId, err := suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)

	err = suite.q.Bury(context.Background(), taskId)
	suite.Require().Nil(err)

	l, err := suite.redis.LLen("req_list_taken" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(0, l)
	l, err = suite.redis.SCard("req_set_buried" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(1, l)
	id, err := suite.redis.SRandMember("req_set_buried" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(taskId, id)
}

func (suite *ReqOpsTestSuite) TestKick() {
	_, err := suite.q.Put(context.Background(), "abc", 0)
	suite.Require().Nil(err)

	var dst string
	taskId, err := suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)

	err = suite.q.Bury(context.Background(), taskId)
	suite.Require().Nil(err)

	err = suite.q.Kick(context.Background(), taskId)
	suite.Require().Nil(err)

	l, err := suite.redis.SCard("req_set_buried" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(0, l)
	l, err = suite.redis.LLen("req_list_ready" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(1, l)
	l, err = suite.redis.LLen("req_list_taken" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(0, l)
	id, err := suite.q.Take(context.Background(), &dst)
	suite.Require().Nil(err)
	suite.Equal(taskId, id)
}

func (suite *ReqOpsTestSuite) TestKickAll() {
	for _, obj := range []string{
		"abc", "def", "ghi",
	} {
		_, err := suite.q.Put(context.Background(), obj, 0)
		suite.Require().Nil(err)

		var dst string
		taskId, err := suite.q.Take(context.Background(), &dst)
		suite.Require().Nil(err)

		err = suite.q.Bury(context.Background(), taskId)
		suite.Require().Nil(err)
	}

	l, err := suite.redis.SCard("req_set_buried" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(3, l)
	l, err = suite.redis.LLen("req_list_ready" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(0, l)

	err = suite.q.KickAll(context.Background())
	suite.Require().Nil(err)

	l, err = suite.redis.SCard("req_set_buried" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(0, l)
	l, err = suite.redis.LLen("req_list_ready" + suite.q.GetId()).Result()
	suite.Require().Nil(err)
	suite.EqualValues(3, l)
}

func TestReqOpsTestSuite(t *testing.T) {
	suite.Run(t, new(ReqOpsTestSuite))
}
