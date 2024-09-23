package main

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

const (
	EVENTSTREAM = "events"
)

type Event struct {
	Name string
}

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

	// Declare the Stream, the segment size and max bytes
	err = env.DeclareStream(EVENTSTREAM,
		stream.NewStreamOptions().
			SetMaxSegmentSizeBytes(stream.ByteCapacity{}.MB(1)).
			SetMaxLengthBytes(stream.ByteCapacity{}.MB(2)))
	if err != nil {
		panic(err)
	}

	// Start a new Producer
	producerOptions := stream.NewProducerOptions()
	producerOptions.SetProducerName("producer")
	producerOptions.SetSubEntrySize(100)
	producerOptions.SetCompression(stream.Compression{}.Gzip())
	producer, err := env.NewProducer(EVENTSTREAM, producerOptions)
	if err != nil {
		panic(err)
	}
	// Publish 6001 messages
	for i := 0; i <= 6001; i++ {

		event := Event{
			Name: "test",
		}
		data, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}
		message := amqp.NewMessage(data)

		// Set the CorrelationID
		props := &amqp.MessageProperties{
			CorrelationID: uuid.NewString(),
		}
		message.Properties = props
		err = producer.Send(message)

		if err != nil {
			panic(err)
		}
	}

	producer.Close()
}
