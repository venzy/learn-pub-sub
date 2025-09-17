package main

import (
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/venzy/learn-pub-sub/internal/gamelogic"
	"github.com/venzy/learn-pub-sub/internal/pubsub"
	"github.com/venzy/learn-pub-sub/internal/routing"
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

	logCh, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open log channel: %s\n", err)
		return
	}
	defer logCh.Close()

	fmt.Println("Move channel opened successfully")
	moveCh, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open move channel: %s\n", err)
		return
	}
	defer moveCh.Close()
	fmt.Println("Move channel opened successfully")

	warCh, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open war channel: %s\n", err)
		return
	}
	defer warCh.Close()
	fmt.Println("War channel opened successfully")

	gameState := gamelogic.NewGameState(username)

	// Each client gets its own pause queue, and the direct exchange routes
	// a copy of every pause message to each queue
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

	// Each client gets its own move queue, and the topic exchange routes
	// a copy of every move message to each queue
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix + "." + username,
		routing.ArmyMovesPrefix + ".*",
		pubsub.TransientQueue,
		handlerMove(gameState, warCh),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to move messages: %s\n", err)
		return
	}

	// All clients share a single war queue, and the topic exchange routes
	// a copy of every war message to this single queue.
	// Clients are called in a round-robin fashion.
	// Each client then decides if it is involved in the war or not, and
	// NackRequeues if not, so the next client can receive it.
	// Not an efficient design, but helps demostrate NackRequeue.
	// A better design would be to have the server route war messages
	// to only the involved clients, but that is more complex to implement.
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix + ".*",
		pubsub.DurableQueue,
		handlerWar(gameState, logCh),
	)
	if err != nil {
		fmt.Printf("Failed to subscribe to war messages: %s\n", err)
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
			if len(userInput) < 2 {
				fmt.Printf("Usage: spam <number of messages>\n")
				continue
			}
			count, err := strconv.ParseInt(userInput[1], 10, 64)
			if err != nil || count <= 0 {
				fmt.Printf("Invalid number of messages: %s\n", userInput[1])
				continue
			}
			for range count {
				log := routing.GameLog{
					CurrentTime: time.Now(),
					Username:    username,
					Message:     gamelogic.GetMaliciousLog(),
				}
				err = pubsub.PublishGob(
					logCh,
					routing.ExchangePerilTopic,
					routing.GameLogSlug + "." + username,
					log,
				)
				if err != nil {
					fmt.Printf("Failed to publish log: %s\n", err)
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			quit = true
		default:
			fmt.Printf("Unknown command: %s\n", command)
		}
	}
}
