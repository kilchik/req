package main

import (
	"bufio"
	"context"
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

func main()  {
	cfg := &req.Config{
		Addr:     "localhost:6379",
		Password: "",
	}

	ctx := context.Background()
	q, err := req.Connect(ctx, cfg)
	if err != nil {
		log.Fatalf("req: connect: %v", err)
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
