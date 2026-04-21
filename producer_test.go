package smkafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type mockProducerClient struct {
	produceFn  func(msg *kafka.Message, deliveryChan chan kafka.Event) error
	metadataFn func(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
}

func (m *mockProducerClient) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return m.produceFn(msg, deliveryChan)
}

func (m *mockProducerClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	if m.metadataFn == nil {
		return &kafka.Metadata{}, nil
	}
	return m.metadataFn(topic, allTopics, timeoutMs)
}

func (m *mockProducerClient) Flush(_ int) int { return 0 }

func (m *mockProducerClient) Close() {}

func TestProducerProduceManySuccess(t *testing.T) {
	var produced int
	mock := &mockProducerClient{
		produceFn: func(_ *kafka.Message, deliveryChan chan kafka.Event) error {
			produced++
			deliveryChan <- &kafka.Message{}
			return nil
		},
	}

	producer := &Producer{
		client:    mock,
		topic:     "topic",
		partition: PartitionAny,
	}

	err := producer.ProduceMany(context.Background(), [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if produced != 2 {
		t.Fatalf("expected 2 produced messages, got %d", produced)
	}
}

func TestProducerReadiness(t *testing.T) {
	mock := &mockProducerClient{
		produceFn: func(_ *kafka.Message, _ chan kafka.Event) error { return nil },
		metadataFn: func(_ *string, _ bool, _ int) (*kafka.Metadata, error) {
			return &kafka.Metadata{}, nil
		},
	}

	producer := &Producer{
		client:           mock,
		name:             "orders-producer",
		readinessTimeout: time.Second,
	}

	if producer.Name() != "orders-producer" {
		t.Fatalf("unexpected name: %s", producer.Name())
	}
	if !producer.IsReady() {
		t.Fatal("expected producer to be ready")
	}
}

func TestProducerProduceManyQueueError(t *testing.T) {
	expectedErr := errors.New("queue failed")
	mock := &mockProducerClient{
		produceFn: func(_ *kafka.Message, _ chan kafka.Event) error {
			return expectedErr
		},
	}

	producer := &Producer{
		client:    mock,
		topic:     "topic",
		partition: PartitionAny,
	}

	err := producer.ProduceMany(context.Background(), [][]byte{[]byte("a")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
