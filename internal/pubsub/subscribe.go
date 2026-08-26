package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack AckType = "ack"
	NackRequeue AckType = "nackrequeue"
	NackDiscard AckType = "nackdiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	feed, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for item := range feed {
			var body T
			if err := json.Unmarshal(item.Body, &body); err != nil {
				log.Printf("error unmarshalling data: %s", err)
				continue
			}
			ackType := handler(body)
			switch ackType {
			case Ack:
				item.Ack(false)
				log.Printf("AckType Ack: %s", item.Body)
			case NackRequeue:
				item.Nack(false, true)
				log.Printf("AckType NackRequeue: %s", item.Body)
			case NackDiscard:
				item.Nack(false, false)
				log.Printf("AckType NackDiscard: %s", item.Body)
			}
		}
	}()

	return nil
}
