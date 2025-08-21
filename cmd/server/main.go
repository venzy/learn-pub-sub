package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	// Create a context that cancels on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("Running... Press Ctrl+C to exit.")

	// Block until signal is received - Done() returns a channel that is closed when the context is canceled
	<-ctx.Done()

	fmt.Println("Received shutdown signal, cleaning up...")
}
