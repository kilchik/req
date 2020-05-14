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
q, err := fabriq.Create(ctx, createOpts...)
```

And you are ready to go.

```
type task struct {
    Body string
}

taskId, err := suite.q.Put(context.Background(), &task{"Get things done"}, 0)

var dst task
id, err := suite.q.Take(context.Background(), &dst)
```
