package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""))
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	// Set prefetchCount to 50 to allow 50 messages before Acks are returned
	if err := ch.Qos(50, 0, false); err != nil {
		panic(err)
	}

	// Auto ACK has to be False, AutoAck = True will Crash dueue to it is no implemented in Streams
	stream, err := ch.Consume("events", "events_consumer", false, false, false, false, amqp.Table{
		"x-stream-offset": 2000,
	})
	if err != nil {
		panic(err)
	}
	// Loop forever and read messages
	fmt.Println("Starting to consume")
	for event := range stream {
		fmt.Printf("Event: %s \n", event.CorrelationId)
		fmt.Printf("headers: %v \n", event.Headers)
		// This is your message payload in the event.Body as a []byte
		fmt.Printf("data: %v \n", string(event.Body))
		event.Ack(true)
	}
	ch.Close()
}
