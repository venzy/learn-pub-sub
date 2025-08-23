package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	ch, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %s\n", err)
		return
	}
	defer ch.Close()
	fmt.Println("Channel opened successfully")

	playingState := routing.PlayingState{IsPaused: true}
	pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playingState)

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
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playingState)
		case "resume":
			fmt.Println("Resuming the game...")
			playingState.IsPaused = false
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playingState)
		case "quit":
			fmt.Println("Quitting the server...")
			quit = true
		default:
			fmt.Printf("Unknown command: %s\n", command)
			gamelogic.PrintServerHelp()
		}
	}
}
