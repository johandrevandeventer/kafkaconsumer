package kafkaconsumer

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// type OutputChannels map[string]chan any

type WorkerFunc func(ctx context.Context, msg Payload, workerLogger *zap.Logger)

type KafkaConsumer struct {
	consumer      *kafka.Consumer
	kafkaTopic    string
	topicPrefix   string
	kafkaLogger   *zap.Logger
	workersLogger *zap.Logger
	worker        WorkerFunc
	poolSize      int
}

// Define the struct you want to pass
type Payload struct {
	MqttTopic        string    `json:"mqtt_topic"`
	Message          string    `json:"message"`
	MessageTimestamp time.Time `json:"message_timestamp"`
}
