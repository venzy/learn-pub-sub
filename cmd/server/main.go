package main

import (
	"fmt"

	"github.com/venzy/learn-pub-sub/internal/gamelogic"
	"github.com/venzy/learn-pub-sub/internal/pubsub"
	"github.com/venzy/learn-pub-sub/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionStr := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		return
	}
	defer connection.Close()
	fmt.Println("Connected to RabbitMQ")

	pauseCh, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %s\n", err)
		return
	}
	defer pauseCh.Close()
	fmt.Println("Channel opened successfully")

	playingState := routing.PlayingState{IsPaused: true}
	err = pubsub.PublishJSON(pauseCh, routing.ExchangePerilDirect, routing.PauseKey, playingState)
	if err != nil {
		fmt.Printf("Failed to publish initial pause state: %s\n", err)
		return
	}

	err = pubsub.SubscribeGob(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug + ".*",
		pubsub.DurableQueue,
		handlerLogMessage(),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to game log messages: %s\n", err)
		return
	}

	fmt.Println("Running... Use 'quit' to exit.")

	// Hint to the user about server commands
	gamelogic.PrintServerHelp()

	quit := false
	for !quit {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		command := userInput[0]
		switch command {
		case "pause":
			fmt.Println("Pausing the game...")
			playingState.IsPaused = true
			pubsub.PublishJSON(pauseCh, routing.ExchangePerilDirect, routing.PauseKey, playingState)
		case "resume":
			fmt.Println("Resuming the game...")
			playingState.IsPaused = false
			pubsub.PublishJSON(pauseCh, routing.ExchangePerilDirect, routing.PauseKey, playingState)
		case "quit":
			fmt.Println("Quitting the server...")
			quit = true
		default:
			fmt.Printf("Unknown command: %s\n", command)
			gamelogic.PrintServerHelp()
		}
	}
}