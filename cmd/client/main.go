package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	// Declare and bind the queue for receiving messages
	ch, q, err := pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey + "." + username,
		routing.PauseKey,
		pubsub.TransientQueue,
	)
	if err != nil {
		fmt.Printf("Failed to declare and bind queue: %s\n", err)
		return
	}

	fmt.Printf("Queue %s declared and bound successfully to channel %s\n", q.Name, ch)

	// Create a context that cancels on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Block until signal is received - Done() returns a channel that is closed when the context is canceled
	<-ctx.Done()

	fmt.Println("Running... Press Ctrl+C to exit.")

	fmt.Println("Received shutdown signal, cleaning up...")
}
