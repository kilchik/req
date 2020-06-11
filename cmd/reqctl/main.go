package main

import (
	"context"
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
	"github.com/kilchik/req/pkg/req"
)

func main() {
	host := flag.String("host", "localhost:6379", "Specify redis host")
	passwd := flag.String("password", "", "Specify redis password")
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

	connectOpts := []func(f *req.Fabriq) error{req.DisableLogger}
	if *sentinels != "" {
		sentinelsArr := strings.Split(*sentinels, ";")
		connectOpts = append(connectOpts, req.UseSentinel(*masterName, *passwd, sentinelsArr))
	} else if *host != "" {
		connectOpts = append(connectOpts, req.SetRedis(*host, *passwd))
	}

	ctx := context.Background()
	fabriq, err := req.Connect(ctx, connectOpts...)
	if err != nil {
		log.Fatalf("reqctl: connect to redis: %v", err)
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
		}
		fmt.Println()
	}
}

func formatStat(s *req.Stat) string {
	return fmt.Sprintf("%-10s%7d\n%-10s%7d\n%-10s%7d\n%-10s%7d\n%-10s%7d",
		"done", s.Done, "ready", s.Ready, "taken", s.Taken, "delayed", s.Delayed, "buried", s.Buried)
}
