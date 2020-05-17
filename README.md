![tests](https://github.com/kilchik/req/workflows/tests/badge.svg)

**req** is redis-based reliable queue with sentinel failover support and at-least-once delivery guarantee.  
The package implements fat client to redis and does not rely on any specific redis procedures except basic type operations.  
**reqctl** is CLI utility that provides user with all the commands listed below.  

Commands interface:
- **put**  
Puts task into queue with delay in seconds.

- **take**  
Gets next FIFO task. The task will be returned to queue if no ack, delete, delay or bury command is received within timeout.
Timeout is specified with SetTakeTimeout create option.

- **ack**  
Acknowledges task completion.

- **delete**  
Deletes task from queue.

- **delay**  
Delays task for next period which is computed as exponential backoff with jitter.

- **bury**  
Moves task into buried set. Will be taken only on kick or kickall command. Helpful for manual error handling or delaying tasks until some service is restored.

- **kick**  
Moves task back to ready list.

- **kickall**  
Moves all tasks to ready list.

- **stat**  
Returns statistics on processed tasks.

# Getting started

```
go get github.com/kilchik/req
```

First you need to initialize queue fabric that will create different queues within a single redis instance.

```
fabriq, err := req.Connect(ctx, connectOpts...)
```

Connect options include:
- specifying redis host via `req.SetRedis`
- specifying sentinel master via `req.UseSentinel`

You can use the package in both synchronous and asynchronous manner:

```
q, err := fabriq.Create(ctx, createOpts...)

type task struct {
    Body string
}

taskId, err := q.Put(ctx, &task{"Get things done"}, time.Second)

var dst task
id, err := q.Take(context.Background(), &dst)
```

or

```
func handleTask(ctx context.Context, taskId string, taskIface interface{}) error {
    t := taskIface.(*task)
    ...
}

asynq, err := fabriq.CreateWithHandler(ctx, &task{}, handleTask)
err := asynq.Put(ctx, &task{"Get things done"}, time.Second)
```

If you choose `AsynQ`, you can control the task flow by returning typed errors from handler:
- by returning `req.NewErrorFatal(fmt string, args ...interface{})` you make sure that the task will be deleted from queue
- by returning `req.NewErrorTemp(fmt string, args ...interface{})` you can delay tasks that can't be executed right now
- by returning `req.NewErrorUnknown(fmt string, args ...interface{})` you can bury tasks for manual parsing

If handler returns `nil` the task is acked.

For a complete async example please look at _cmd/examples_.
