package main

import (
	"fmt"

	"github.com/venzy/learn-pub-sub/internal/gamelogic"
	"github.com/venzy/learn-pub-sub/internal/pubsub"
	"github.com/venzy/learn-pub-sub/internal/routing"
)

func handlerLogMessage() func(msg *routing.GameLog) pubsub.AckType {
	return func(msg *routing.GameLog) pubsub.AckType {
		// Print the log message to the console
		// Note: We print a newline first to ensure it appears correctly
		// if the user is typing a command
		defer fmt.Printf("\n[%s] %s: %s\n> ", msg.CurrentTime.Format("15:04:05"), msg.Username, msg.Message)
		err := gamelogic.WriteLog(*msg)
		if err != nil {
			fmt.Printf("Failed to write log message to file: %s\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}