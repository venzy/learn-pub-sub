package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type simpleQueueType int

const (
	// DurableQueue indicates a durable queue that survives server restarts
	DurableQueue simpleQueueType = iota
	// TransientQueue indicates a transient queue that does not survive server restarts
	TransientQueue
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType simpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	connCh, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	q, err := connCh.QueueDeclare(
		queueName,
		queueType == DurableQueue, // durable if DurableQueue
		queueType == TransientQueue, // delete if TransientQueue
		queueType == TransientQueue, // exclusive if TransientQueue
		false, // noWait is false
		nil,   // arguments are empty
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = connCh.QueueBind(
		q.Name,
		key,
		exchange,
		false,
		nil, // no arguments
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return connCh, q, nil
}