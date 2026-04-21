package smkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const PartitionAny int32 = kafka.PartitionAny
const defaultReadinessTimeout = 3 * time.Second

type Producer struct {
	client           producerClient
	name             string
	topic            string
	partition        int32
	readinessTimeout time.Duration
}

func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("producer topic must not be empty")
	}

	kafkaCfg, err := producerKafkaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build producer config: %w", err)
	}

	client, err := kafka.NewProducer(&kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	partition := PartitionAny
	if cfg.Partition != nil {
		partition = *cfg.Partition
	}

	name := cfg.Name
	if name == "" {
		name = "smkafka-producer"
	}

	readinessTimeout := cfg.ReadinessTimeout
	if readinessTimeout <= 0 {
		readinessTimeout = defaultReadinessTimeout
	}

	return &Producer{
		client:           client,
		name:             name,
		topic:            cfg.Topic,
		partition:        partition,
		readinessTimeout: readinessTimeout,
	}, nil
}

func (p *Producer) Name() string {
	return p.name
}

func (p *Producer) IsReady() bool {
	timeoutMs := int(p.readinessTimeout.Milliseconds())
	if timeoutMs <= 0 {
		timeoutMs = int(defaultReadinessTimeout.Milliseconds())
	}
	_, err := p.client.GetMetadata(nil, false, timeoutMs)
	return err == nil
}

func (p *Producer) Produce(ctx context.Context, message []byte) error {
	return p.produceOne(ctx, message)
}

func (p *Producer) ProduceMany(ctx context.Context, messages [][]byte) error {
	if len(messages) == 0 {
		return nil
	}

	deliveryChan := make(chan kafka.Event, len(messages))

	for index, message := range messages {
		err := p.client.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &p.topic,
				Partition: p.partition,
			},
			Value: message,
		}, deliveryChan)
		if err != nil {
			return fmt.Errorf("queue message #%d: %w", index, err)
		}
	}

	for delivered := 0; delivered < len(messages); delivered++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for batch delivery: %w", ctx.Err())
		case event := <-deliveryChan:
			delivery, ok := event.(*kafka.Message)
			if !ok {
				return fmt.Errorf("unexpected producer delivery event type %T", event)
			}

			if delivery.TopicPartition.Error != nil {
				return fmt.Errorf("batch delivery failed: %w", delivery.TopicPartition.Error)
			}
		}
	}

	return nil
}

func (p *Producer) produceOne(ctx context.Context, message []byte) error {
	deliveryChan := make(chan kafka.Event, 1)

	err := p.client.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: p.partition,
		},
		Value: message,
	}, deliveryChan)
	if err != nil {
		return fmt.Errorf("produce message: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("wait for message delivery: %w", ctx.Err())
	case event := <-deliveryChan:
		delivery, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected producer delivery event type %T", event)
		}

		if delivery.TopicPartition.Error != nil {
			return fmt.Errorf("delivery failed: %w", delivery.TopicPartition.Error)
		}
	}

	return nil
}

func (p *Producer) Flush(timeout time.Duration) int {
	if timeout < 0 {
		timeout = 0
	}
	return p.client.Flush(int(timeout.Milliseconds()))
}

func (p *Producer) Close() {
	p.client.Close()
}
