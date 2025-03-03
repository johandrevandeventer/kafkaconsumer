package kafkaconsumer

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/johandrevandeventer/persist"
	"go.uber.org/zap"
)

type OutputChannels map[string]chan any

type WorkerFunc func(msg Payload, outputChannels OutputChannels, statePersister *persist.FilePersister)

type KafkaConsumer struct {
	consumer       *kafka.Consumer
	kafkaTopic     string
	topicPrefix    string
	logger         *zap.Logger
	worker         WorkerFunc
	poolSize       int
	outputChannels OutputChannels
	statePersister persist.FilePersister
}

// Define the struct you want to pass
type Payload struct {
	MqttTopic        string    `json:"mqtt_topic"`
	Message          string    `json:"message"`
	MessageTimestamp time.Time `json:"message_timestamp"`
}
