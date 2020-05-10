package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

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

	q, err := fabriq.Create(ctx, createOpts...)
	if err != nil {
		log.Fatalf("reqctl: create queue: %v", err)
	}

	rdr := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		str, err := rdr.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Fprintf(os.Stderr, "read line: %v", err)
		}
		if str == "" {
			continue
		}
		tokens := strings.Split(str[:len(str)-1], " ")
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
		}
	}
}
