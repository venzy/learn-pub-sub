package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	// DurableQueue indicates a durable queue that survives server restarts
	DurableQueue SimpleQueueType = iota
	// TransientQueue indicates a transient queue that does not survive server restarts
	TransientQueue
)

// AckType represents the acknowledgment type for message processing
type AckType int

const (
	// Ack indicates that a message was processed successfully
	Ack AckType = iota
	// NackRequeue indicates that a message was not processed successfully and should be requeued
	NackRequeue
	// NackDiscard indicates that a message was not processed successfully and should be discarded
	NackDiscard
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
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
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"}, // arguments
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

func SubscribeJSON[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		var val T
		err := json.Unmarshal(data, &val)
		return val, err
	})
}

func SubscribeGob[T any](
    conn *amqp.Connection,
    exchange,
    queueName,
    key string,
    queueType SimpleQueueType, // an enum to represent "durable" or "transient"
    handler func(T) AckType,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, func(data []byte) (T, error) {
		reader := bytes.NewReader(data)
		dec := gob.NewDecoder(reader)
		var val T
		err := dec.Decode(&val)
		return val, err
	})
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	connCh, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = connCh.Qos(10, 0, false) // prefetch count of 10
	if err != nil {
		return err
	}
	msgs, err := connCh.Consume(
		q.Name,
		"",
		false, // autoAck is false
		false, // exclusive is false
		false, // noLocal is false
		false, // noWait is false
		nil,   // arguments are nil
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			val, err := unmarshaller(msg.Body)
			if err != nil {
				msg.Nack(false, false) // nack the message if unmarshal fails
				continue
			}
			switch (handler(val)) {
			case Ack:
				fmt.Println("Acking message")
				msg.Ack(false) // ack the message after processing
			case NackRequeue:
				fmt.Println("Nacking message (requeue)")
				msg.Nack(false, true) // nack the message and requeue
			case NackDiscard:
				fmt.Println("Nacking message (discard)")
				msg.Nack(false, false) // nack the message and discard
			}
		}
	}()

	return nil
}