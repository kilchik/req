package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/kilchik/req/pkg/req"
)

type SumPositiveNumbersTask struct {
	A int
	B int
}

func handleSumPositiveNumbersTask(ctx context.Context, taskId string, taskIface interface{}) error {
	task := taskIface.(*SumPositiveNumbersTask)
	if task.A < 0 || task.B < 0 {
		// This task should not be executed. By using ErrorFatal we make sure that the task will be deleted from queue
		return req.NewErrorFatal("invalid input")
	}

	resp, _ := http.Get(fmt.Sprintf("http://sum-service/sum?a=%d&b=%d", task.A, task.B))
	if resp.StatusCode == http.StatusServiceUnavailable {
		// This task will be delayed. By using ErrorTemp we can delay tasks that can't be executed right now
		return req.NewErrorTemp("service is temporary unavailable")
	}

	content, _ := ioutil.ReadAll(resp.Body)
	sum, err := strconv.Atoi(string(content))
	if err != nil {
		// This task will be buried until 'kick' called. By using ErrorUnknown we can bury tasks for manual parsing.
		return req.NewErrorUnknown("unexpected response content: %q (taskId=%q)", string(content), taskId)
	}

	fmt.Println(sum)

	// This task will be acked.
	return nil
}

func main() {
	ctx := context.Background()

	f, _ := req.Connect(ctx)
	asynq, _ := f.OpenWithHandler(ctx, &SumPositiveNumbersTask{}, handleSumPositiveNumbersTask, req.SetName("summer"))

	if err := asynq.Put(ctx, SumPositiveNumbersTask{A: 40, B: 2}, time.Second); err != nil {
		log.Fatalf("put new sum task: %v", err)
	}
}
