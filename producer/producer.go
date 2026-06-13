package producer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/overiss/smkafka/internal/shared"
)

const PartitionAny int32 = kafka.PartitionAny

type Producer struct {
	client           client
	name             string
	topic            string
	partition        int32
	readinessTimeout time.Duration
	hooks            Hooks
}

func New(cfg Config) (*Producer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("producer topic must not be empty")
	}

	kafkaCfg, err := kafkaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build producer config: %w", err)
	}

	c, err := kafka.NewProducer(&kafkaCfg)
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
		readinessTimeout = shared.DefaultReadinessTimeout
	}

	return &Producer{
		client:           c,
		name:             name,
		topic:            cfg.Topic,
		partition:        partition,
		readinessTimeout: readinessTimeout,
		hooks:            cfg.Hooks,
	}, nil
}

func (p *Producer) Name() string {
	return p.name
}

func (p *Producer) IsReady() bool {
	timeoutMs := int(p.readinessTimeout.Milliseconds())
	if timeoutMs <= 0 {
		timeoutMs = int(shared.DefaultReadinessTimeout.Milliseconds())
	}
	_, err := p.client.GetMetadata(nil, false, timeoutMs)
	return err == nil
}

func (p *Producer) Produce(ctx context.Context, key, message []byte) (err error) {
	start := time.Now()
	defer func() { shared.CallHook(p.hooks.OnProduce, start, err) }()

	return p.produceOne(ctx, key, message)
}

func (p *Producer) ProduceMany(ctx context.Context, key []byte, messages [][]byte) (err error) {
	start := time.Now()
	defer func() { shared.CallHook(p.hooks.OnProduceMany, start, err) }()

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
			Key:   key,
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

func (p *Producer) produceOne(ctx context.Context, key, message []byte) error {
	deliveryChan := make(chan kafka.Event, 1)

	err := p.client.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: p.partition,
		},
		Key:   key,
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
