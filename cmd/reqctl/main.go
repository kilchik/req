package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
	"github.com/google/uuid"
	"github.com/kilchik/req/pkg/fabriq"
	"github.com/kilchik/req/pkg/req"
	"github.com/kilchik/req/pkg/types"
)

func main() {
	host := flag.String("host", "localhost:6379", "Specify storage host")
	passwd := flag.String("password", "", "Specify storage password")
	qname := flag.String("qname", "default", "Specify queue name")
	sentinels := flag.String("sentinels", "", "Specify sentinels list using ; as separator")
	masterName := flag.String("master", "", "Specify sentinel master name")

	flag.Parse()

	if *sentinels != "" {
		if *masterName == "" {
			fmt.Println("you must specify master name to use sentinel mode")
			return
		}
	}

	if *masterName != "" {
		if *sentinels == "" {
			fmt.Println("you must specify sentinels list to use sentinel mode")
			return
		}
	}

	connectOpts := []func(f *fabriq.Fabriq) error{fabriq.DisableLogger}
	if *sentinels != "" {
		sentinelsArr := strings.Split(*sentinels, ";")
		connectOpts = append(connectOpts, fabriq.UseSentinel(*masterName, *passwd, sentinelsArr))
	} else if *host != "" {
		connectOpts = append(connectOpts, fabriq.SetRedis(*host, *passwd))
	}

	ctx := context.Background()
	fabriq, err := fabriq.Connect(ctx, connectOpts...)
	if err != nil {
		log.Fatalf("reqctl: connect to storage: %v", err)
	}

	var createOpts []func(q *req.Q) error
	if *qname != "default" {
		createOpts = append(createOpts, req.SetName(*qname))
	}

	q, err := fabriq.Open(ctx, createOpts...)
	if err != nil {
		log.Fatalf("reqctl: create queue: %v", err)
	}

	lineRdr, err := readline.NewEx(&readline.Config{
		Prompt:            "> ",
		HistoryFile:       "/tmp/reqctl_history.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "exit",
		HistorySearchFold: true,
	})
	if err != nil {
		log.Fatalf("reqctl: create line reader")
	}

	for {
		str, err := lineRdr.Readline()
		if err != nil {
			if err != readline.ErrInterrupt && err != io.EOF {
				fmt.Fprintf(os.Stderr, "read line: %v", err)
			}
			break
		}

		if str == "" {
			continue
		}
		tokens := strings.Split(str, " ")
		switch tokens[0] {
		case "put":
			if len(tokens) != 3 {
				printError("invalid args")
				continue
			}
			delay, err := strconv.Atoi(tokens[2])
			if err != nil {
				printError("invalid delay")
				continue
			}
			taskBody := tokens[1]
			taskId, err := q.Put(ctx, taskBody, time.Duration(delay)*time.Second)
			if err != nil {
				printError("put: %v", err)
				continue
			}
			printSuccess(taskId)

		case "take":
			if len(tokens) != 1 {
				printError("invalid args")
				continue
			}
			var res string
			taskId, err := q.Take(ctx, &res)
			if err != nil {
				printError("take: %v", err)
				continue
			}
			printSuccess(taskId + " " + res)

		case "ack":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			if _, err := uuid.Parse(tokens[1]); err != nil {
				printError("invalid uuid")
				continue
			}
			if err := q.Ack(ctx, tokens[1]); err != nil {
				printError("ack: %v", err)
			}

		case "delete":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			if _, err := uuid.Parse(tokens[1]); err != nil {
				printError("invalid uuid")
				continue
			}
			if err := q.Delete(ctx, tokens[1]); err != nil {
				printError("delete: %v", err)
			}

		case "delete-buried":
			if len(tokens) < 2 {
				printError("expected one or more uuids")
				continue
			}
			if tokens[1] == "*" {
				for {
					top, err := q.Top(ctx, req.StateBuried, 100)
					if err != nil {
						printError("get top 200 buried: %v", err)
						break
					}
					if len(top) == 0 {
						break
					}
					for _, t := range top {
						if err := q.DeleteBuried(ctx, t); err != nil {
							printError("delete buried task %q: %v", t, err)
						}
					}
					printSuccess("deleted %d buried tasks", len(top))
				}
			} else {
				for _, t := range tokens[1:] {
					if _, err := uuid.Parse(t); err != nil {
						printError("invalid uuid %q", t)
						continue
					}
					if err := q.DeleteBuried(ctx, t); err != nil {
						printError("delete buried task %q: %v", t, err)
					}
				}
			}

		case "delay":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			if _, err := uuid.Parse(tokens[1]); err != nil {
				printError("invalid uuid")
				continue
			}
			if err := q.Delay(ctx, tokens[1]); err != nil {
				printError("delay: %v", err)
			}

		case "stat":
			if len(tokens) != 1 {
				printError("invalid args")
				continue
			}
			s, err := q.Stat(ctx)
			if err != nil {
				printError("stat: %v", err)
				continue
			}
			printSuccess(formatStat(s))

		case "bury":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			if _, err := uuid.Parse(tokens[1]); err != nil {
				printError("invalid uuid")
				continue
			}
			if err := q.Bury(ctx, tokens[1]); err != nil {
				printError("bury: %v", err)
			}

		case "kick":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			if _, err := uuid.Parse(tokens[1]); err != nil {
				printError("invalid uuid")
				continue
			}
			if err := q.Kick(ctx, tokens[1]); err != nil {
				printError("kick: %v", err)
			}

		case "kickall":
			if len(tokens) != 1 {
				printError("invalid args")
				continue
			}
			if err := q.KickAll(ctx); err != nil {
				printError("kick all: %v", err)
			}

		case "top":
			validStates := map[string]req.TaskState{
				"delayed": req.StateDelayed,
				"ready":   req.StateReady,
				"taken":   req.StateTaken,
				"buried":  req.StateBuried,
			}
			invalidStateMsg := "expected state delayed/ready/taken/buried"
			if len(tokens) < 2 {
				printError(invalidStateMsg)
				continue
			}
			state, present := validStates[tokens[1]]
			if !present {
				printError(invalidStateMsg)
				continue
			}
			var size int64 = 5
			if len(tokens) > 2 {
				size, err = strconv.ParseInt(tokens[2], 10, 64)
				if err != nil {
					printError("invalid size")
					continue
				}
			}
			tids, err := q.Top(ctx, state, size)
			if err != nil {
				printError("get top from queue: %v", err)
				continue
			}
			printSuccess(strings.Join(tids, " "))

		case "watch":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			if _, err := uuid.Parse(tokens[1]); err != nil {
				printError("invalid uuid")
				continue
			}
			task, err := q.Watch(ctx, tokens[1])
			if err != nil {
				printError("watch: %v", err)
				continue
			}
			printSuccess(formatTask(task))

		case "find":
			if len(tokens) != 2 {
				printError("invalid args")
				continue
			}
			tokens = strings.Split(tokens[1], "=")
			if len(tokens) != 2 {
				printError("invalid expression")
				continue
			}
			pattern := strings.Join(tokens, `":`)
			tasks, err := q.Find(ctx, pattern)
			if err != nil {
				printError("find: %v", err)
				continue
			}
			stateNames := map[req.TaskState]string{
				req.StateDelayed: "DELAYED",
				req.StateReady:   "READY",
				req.StateTaken:   "TAKEN",
				req.StateBuried:  "BURIED",
			}
			for _, state := range []req.TaskState{req.StateBuried, req.StateDelayed, req.StateReady, req.StateTaken} {
				if _, exists := tasks[state]; exists {
					printSuccess(stateNames[state])
				}
				for _, t := range tasks[state] {
					printSuccess(formatTask(t))
				}
			}
		}

		fmt.Println()
	}
}

func formatStat(s *req.Stat) string {
	return fmt.Sprintf("%-10s%7d\n%-10s%7d\n%-10s%7d\n%-10s%7d\n%-10s%7d",
		"done", s.Done, "ready", s.Ready, "taken", s.Taken, "delayed", s.Delayed, "buried", s.Buried)
}

type Task struct {
	Id      string
	Delay   time.Duration
	Body    map[string]interface{}
	TakenAt time.Time
}

func formatTask(t *types.Task) string {
	body := make(map[string]interface{})
	json.Unmarshal(t.Body, &body)
	res := &Task{
		Id:      t.Id,
		Delay:   t.Delay,
		Body:    body,
		TakenAt: t.TakenAt,
	}

	taskBytes, _ := json.MarshalIndent(res, "", "\t")

	return string(taskBytes)
}
