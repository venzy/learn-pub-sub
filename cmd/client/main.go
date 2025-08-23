package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionStr := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionStr)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		return
	}
	defer connection.Close()
	fmt.Println("Connected to RabbitMQ")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error during welcome: %s\n", err)
		return
	}

	moveCh, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %s\n", err)
		return
	}
	defer moveCh.Close()
	fmt.Println("Move channel opened successfully")

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gameState),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to pause messages: %s\n", err)
		return
	}

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + username,
		routing.ArmyMovesPrefix + ".*",
		pubsub.TransientQueue,
		handlerMove(gameState),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to move messages: %s\n", err)
		return
	}

	quit := false
	for !quit {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}
		command := userInput[0]
		switch command {
		case "spawn":
			err := gameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Printf("%s\n", err)
			}
		case "move":
			armyMove, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Printf("%s\n", err)
				continue
			}
			err = pubsub.PublishJSON(
				moveCh,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix + "." + username,
				armyMove,
			)
			if err != nil {
				fmt.Printf("Failed to publish move: %s\n", err)
			} else {
				fmt.Printf("Move published successfully: %v\n", armyMove)
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Printf("Spamming not allowed yet!\n")
		case "quit":
			gamelogic.PrintQuit()
			quit = true
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(am gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(am)
	}
}