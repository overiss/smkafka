package smkafka

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

type producerClient interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
	Close()
}

type consumerClient interface {
	Poll(timeoutMs int) kafka.Event
	Commit() ([]kafka.TopicPartition, error)
	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	Assignment() ([]kafka.TopicPartition, error)
	OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error)
	Resume(partitions []kafka.TopicPartition) error
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Close() error
}
