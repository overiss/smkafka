package producer

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type client interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	Flush(timeoutMs int) int
	Close()
}
