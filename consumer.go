package smkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	defaultBatchSize     = 100
	defaultBatchDeadline = 5 * time.Second
	defaultReconnectWait = 5 * time.Second
)

type Batch struct {
	Messages [][]byte
}

type Consumer struct {
	client           consumerClient
	name             string
	defaultMaxSize   int
	defaultBatchWait time.Duration
	reconnectWait    time.Duration
	readinessTimeout time.Duration

	mu           sync.Mutex
	lastBatchRaw []*kafka.Message
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("consumer topic must not be empty")
	}

	kafkaCfg, err := consumerKafkaConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build consumer config: %w", err)
	}

	client, err := kafka.NewConsumer(&kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	if err := client.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
		client.Close()
		return nil, fmt.Errorf("subscribe topic %q: %w", cfg.Topic, err)
	}

	defaultMaxSize := cfg.BatchSize
	if defaultMaxSize <= 0 {
		defaultMaxSize = defaultBatchSize
	}

	defaultBatchWait := cfg.BatchDeadline
	if defaultBatchWait <= 0 {
		defaultBatchWait = defaultBatchDeadline
	}

	reconnectWait := cfg.ReconnectTimeout
	if reconnectWait <= 0 {
		reconnectWait = defaultReconnectWait
	}

	name := cfg.Name
	if name == "" {
		name = "smkafka-consumer"
	}

	readinessTimeout := cfg.ReadinessTimeout
	if readinessTimeout <= 0 {
		readinessTimeout = defaultReadinessTimeout
	}

	return &Consumer{
		client:           client,
		name:             name,
		defaultMaxSize:   defaultMaxSize,
		defaultBatchWait: defaultBatchWait,
		reconnectWait:    reconnectWait,
		readinessTimeout: readinessTimeout,
	}, nil
}

func (c *Consumer) Name() string {
	return c.name
}

func (c *Consumer) IsReady() bool {
	timeoutMs := int(c.readinessTimeout.Milliseconds())
	if timeoutMs <= 0 {
		timeoutMs = int(defaultReadinessTimeout.Milliseconds())
	}
	_, err := c.client.GetMetadata(nil, false, timeoutMs)
	return err == nil
}

func (c *Consumer) Consume(ctx context.Context) ([]byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		event := c.client.Poll(200)
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			return e.Value, nil
		case kafka.Error:
			if e.Code() == kafka.ErrAssignmentLost {
				continue
			}
			return nil, fmt.Errorf("consume event: %w", e)
		case kafka.PartitionEOF:
			continue
		}
	}
}

func (c *Consumer) ConsumeBatch(ctx context.Context) (Batch, error) {
	deadline := time.Now().Add(c.defaultBatchWait)
	rawMessages := make([]*kafka.Message, 0, c.defaultMaxSize)
	messages := make([][]byte, 0, c.defaultMaxSize)

	for len(rawMessages) < c.defaultMaxSize {
		if err := ctx.Err(); err != nil {
			batch := Batch{Messages: messages}
			c.setLastBatch(rawMessages)
			return batch, err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		pollTimeout := minDuration(remaining, 200*time.Millisecond)
		event := c.client.Poll(int(pollTimeout.Milliseconds()))
		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			rawMessages = append(rawMessages, e)
			messages = append(messages, e.Value)
		case kafka.Error:
			if e.Code() == kafka.ErrAssignmentLost {
				continue
			}
			batch := Batch{Messages: messages}
			c.setLastBatch(rawMessages)
			return batch, fmt.Errorf("consume event: %w", e)
		case kafka.PartitionEOF:
			continue
		}
	}

	c.setLastBatch(rawMessages)
	return Batch{Messages: messages}, nil
}

func (c *Consumer) Commit() error {
	assignmentLost, err := c.commitOnce()
	if err != nil {
		return err
	}
	if !assignmentLost {
		return nil
	}

	if reconnectErr := c.reconnect(); reconnectErr != nil {
		return fmt.Errorf("reconnect after assignment lost: %w", reconnectErr)
	}

	_, err = c.commitOnce()
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) CommitBatch() error {
	batch := c.takeLastBatch()
	if len(batch) == 0 {
		return nil
	}

	offsets := makeOffsetsFromMessages(batch)
	assignmentLost, err := c.commitBatchOnce(offsets)
	if err != nil {
		c.restoreLastBatch(batch)
		return err
	}
	if !assignmentLost {
		return nil
	}

	if reconnectErr := c.reconnect(); reconnectErr != nil {
		c.restoreLastBatch(batch)
		return fmt.Errorf("reconnect after assignment lost: %w", reconnectErr)
	}

	assignmentLost, err = c.commitBatchOnce(offsets)
	if err != nil {
		c.restoreLastBatch(batch)
		return err
	}
	if assignmentLost {
		c.restoreLastBatch(batch)
		return errors.New("batch commit failed after reconnect: assignment lost")
	}

	return nil
}

func (c *Consumer) Close() error {
	return c.client.Close()
}

func makeOffsetsFromMessages(messages []*kafka.Message) []kafka.TopicPartition {
	type partitionKey struct {
		topic     string
		partition int32
	}

	offsetsByPartition := make(map[partitionKey]kafka.TopicPartition, len(messages))

	for _, message := range messages {
		topic := ""
		if message.TopicPartition.Topic != nil {
			topic = *message.TopicPartition.Topic
		}
		key := partitionKey{
			topic:     topic,
			partition: message.TopicPartition.Partition,
		}
		offsetsByPartition[key] = kafka.TopicPartition{
			Topic:     message.TopicPartition.Topic,
			Partition: message.TopicPartition.Partition,
			Offset:    message.TopicPartition.Offset + 1,
		}
	}

	offsets := make([]kafka.TopicPartition, 0, len(offsetsByPartition))
	for _, offset := range offsetsByPartition {
		offsets = append(offsets, offset)
	}

	return offsets
}

func minDuration(first, second time.Duration) time.Duration {
	if first < second {
		return first
	}
	return second
}

func (c *Consumer) setLastBatch(messages []*kafka.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastBatchRaw = messages
}

func (c *Consumer) takeLastBatch() []*kafka.Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.lastBatchRaw) == 0 {
		return nil
	}

	batch := c.lastBatchRaw
	c.lastBatchRaw = nil
	return batch
}

func (c *Consumer) restoreLastBatch(messages []*kafka.Message) {
	if len(messages) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.lastBatchRaw) == 0 {
		c.lastBatchRaw = messages
	}
}

func (c *Consumer) reconnect() error {
	assignments, err := c.client.Assignment()
	if err != nil {
		return fmt.Errorf("read assignments: %w", err)
	}
	if len(assignments) == 0 {
		return nil
	}

	timestamp := time.Now().UnixMilli()
	for i := range assignments {
		assignments[i].Offset = kafka.Offset(timestamp)
	}

	timeoutMs := int(c.reconnectWait.Milliseconds())
	if timeoutMs <= 0 {
		timeoutMs = int(defaultReconnectWait.Milliseconds())
	}

	withLastOffsets, err := c.client.OffsetsForTimes(assignments, timeoutMs)
	if err != nil {
		return fmt.Errorf("resolve offsets for reconnect: %w", err)
	}

	if err = c.client.Resume(withLastOffsets); err != nil {
		return fmt.Errorf("resume assignments: %w", err)
	}

	return nil
}

func (c *Consumer) commitOnce() (assignmentLost bool, err error) {
	_, err = c.client.Commit()
	if isAssignmentLostError(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("commit offsets: %w", err)
	}
	return false, nil
}

func (c *Consumer) commitBatchOnce(offsets []kafka.TopicPartition) (assignmentLost bool, err error) {
	committed, err := c.client.CommitOffsets(offsets)
	if isAssignmentLostError(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("commit batch offsets: %w", err)
	}

	for _, tp := range committed {
		if tp.Error == nil {
			continue
		}
		if isAssignmentLostError(tp.Error) {
			return true, nil
		}
		topic := ""
		if tp.Topic != nil {
			topic = *tp.Topic
		}
		return false, fmt.Errorf("commit partition %s[%d]: %w", topic, tp.Partition, tp.Error)
	}

	return false, nil
}
