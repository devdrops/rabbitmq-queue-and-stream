package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Our struct that we will send
type Event struct {
	Name string
}

func main() {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""))
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	// Start the queue
	q, err := ch.QueueDeclare("events", true, false, false, true, amqp.Table{
		"x-queue-type":                    "stream",
		"x-stream-max-segment-size-bytes": 30000,  // 0.03 MB is the maximum size of each Segment of the Stream
		"x-max-length-bytes":              150000, // 0.15 MB is the maximum length of the Stream
	})
	if err != nil {
		panic(err)
	}

	// Publish 1000 Messages
	ctx := context.Background()
	for i := 0; i <= 1000; i++ {
		event := Event{
			Name: "test nº " + string(i),
		}

		data, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		err = ch.PublishWithContext(ctx, "", "events", false, false, amqp.Publishing{
			CorrelationId: uuid.NewString(),
			Body:          data,
		})
		if err != nil {
			panic(err)
		}
	}
	ch.Close()
	fmt.Println(q.Name)
}
