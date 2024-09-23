package main

import (
	"fmt"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	EVENTSTREAM = "events"
)

func main() {
	// Connect to the server on localhost
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	if err != nil {
		panic(err)
	}
	// Create a Consumer, here we can confiugre our Offset using SetOffset
	consumerOptions := stream.NewConsumerOptions().
		SetConsumerName("consumer_1").
		SetOffset(stream.OffsetSpecification{}.First())

	// Starting the Consumer this way, It will apply messageHandler on each new message
	consumer, err := env.NewConsumer(EVENTSTREAM, messageHandler, consumerOptions)
	if err != nil {
		panic(err)
	}
	// Sleep while we consume, in a regular app handle this as a regular goroutine with a context
	time.Sleep(5 * time.Second)
	consumer.Close()
}

// messageHandler is used to accept messages from the stream and apply business logic
func messageHandler(consumerContext stream.ConsumerContext, event *amqp.Message) {
	fmt.Printf("Event: %s \n", event.Properties.CorrelationID)
	fmt.Printf("data: %v \n", string(event.GetData()))
	// Unmarshal data into your struct here
}
