package kafkaconsumer

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/johandrevandeventer/persist"
	"go.uber.org/zap"
)

// NewKafkaProducer initializes a new Kafka producer instance with a flexible configuration.
func NewKafkaConsumer(config kafka.ConfigMap, kafkaTopic, topicPrefix string, kafkaLogger, workersLogger *zap.Logger, worker WorkerFunc, poolSize int, outputChannels OutputChannels, statePersister persist.FilePersister) *KafkaConsumer {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		kafkaLogger.Fatal("Failed to create consumer", zap.Error(err))
	}

	kafkaLogger.Info("Kafka consumer created successfully")

	return &KafkaConsumer{
		consumer:       consumer,
		kafkaTopic:     kafkaTopic,
		topicPrefix:    topicPrefix,
		kafkaLogger:    kafkaLogger,
		workersLogger:  workersLogger,
		worker:         worker,
		poolSize:       poolSize,
		outputChannels: outputChannels,
		statePersister: statePersister,
	}
}

func (kc *KafkaConsumer) Start() {
	// Subscribe to Kafka topic
	err := kc.consumer.Subscribe(kc.kafkaTopic, nil)
	if err != nil {
		kc.kafkaLogger.Fatal("Error subscribing to topic", zap.Error(err))
		return
	} else {
		kc.kafkaLogger.Info("Successfully subscribed to Kafka topic", zap.String("topic", kc.kafkaTopic))
	}
}

// ConsumeMessages starts consuming messages from the Kafka topic.
func (kc *KafkaConsumer) ConsumeMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			kc.kafkaLogger.Info("Stopping Kafka consumer...")
			kc.consumer.Close()
			return
		default:
			ev := kc.consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				msg := kc.formatPayload(e)
				if len(msg.MqttTopic) >= len(kc.topicPrefix) && msg.MqttTopic[:len(kc.topicPrefix)] == kc.topicPrefix {
					kc.kafkaLogger.Info("Received message", zap.String("kafka_topic", *e.TopicPartition.Topic), zap.String("mqtt_topic", msg.MqttTopic), zap.Int64("offset", int64(e.TopicPartition.Offset)))
					kc.runWorker(kc.worker, msg)
				} else {
					kc.kafkaLogger.Warn("Skipping message", zap.String("kafka_topic", *e.TopicPartition.Topic), zap.String("mqtt_topic", msg.MqttTopic), zap.Int64("offset", int64(e.TopicPartition.Offset)))
				}

			case kafka.PartitionEOF:
				kc.kafkaLogger.Info("Reached end of partition", zap.String("topic", *e.Topic))
			case kafka.Error:
				kc.kafkaLogger.Error("Kafka error", zap.Error(e))
			}
		}
	}
}

func (kc *KafkaConsumer) formatPayload(msg *kafka.Message) Payload {
	var receivedPayload Payload
	err := json.Unmarshal(msg.Value, &receivedPayload)
	if err != nil {
		kc.kafkaLogger.Error("Error unmarshalling message", zap.Error(err))
		return Payload{}
	}

	return receivedPayload
}

// runWorker executes the worker in a goroutine
func (kc *KafkaConsumer) runWorker(worker WorkerFunc, msg Payload) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker(msg, kc.outputChannels, &kc.statePersister, kc.workersLogger)
	}()
	if kc.poolSize > 0 {
		wg.Wait() // Wait for all workers to finish if poolSize > 0
	}
}
