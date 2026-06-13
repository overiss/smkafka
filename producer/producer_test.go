package producer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type mockClient struct {
	produceFn  func(msg *kafka.Message, deliveryChan chan kafka.Event) error
	metadataFn func(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
}

func (m *mockClient) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	return m.produceFn(msg, deliveryChan)
}

func (m *mockClient) GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error) {
	if m.metadataFn == nil {
		return &kafka.Metadata{}, nil
	}
	return m.metadataFn(topic, allTopics, timeoutMs)
}

func (m *mockClient) Flush(_ int) int { return 0 }

func (m *mockClient) Close() {}

func TestProduceManySuccess(t *testing.T) {
	var produced int
	key := []byte("order-key")
	mock := &mockClient{
		produceFn: func(msg *kafka.Message, deliveryChan chan kafka.Event) error {
			produced++
			if string(msg.Key) != string(key) {
				t.Fatalf("expected key %q, got %q", key, msg.Key)
			}
			deliveryChan <- &kafka.Message{}
			return nil
		},
	}

	p := &Producer{
		client:    mock,
		topic:     "topic",
		partition: PartitionAny,
	}

	err := p.ProduceMany(context.Background(), key, [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if produced != 2 {
		t.Fatalf("expected 2 produced messages, got %d", produced)
	}
}

func TestReadiness(t *testing.T) {
	mock := &mockClient{
		produceFn: func(_ *kafka.Message, _ chan kafka.Event) error { return nil },
		metadataFn: func(_ *string, _ bool, _ int) (*kafka.Metadata, error) {
			return &kafka.Metadata{}, nil
		},
	}

	p := &Producer{
		client:           mock,
		name:             "orders-producer",
		readinessTimeout: time.Second,
	}

	if p.Name() != "orders-producer" {
		t.Fatalf("unexpected name: %s", p.Name())
	}
	if !p.IsReady() {
		t.Fatal("expected producer to be ready")
	}
}

func TestProduceManyQueueError(t *testing.T) {
	expectedErr := errors.New("queue failed")
	mock := &mockClient{
		produceFn: func(_ *kafka.Message, _ chan kafka.Event) error {
			return expectedErr
		},
	}

	p := &Producer{
		client:    mock,
		topic:     "topic",
		partition: PartitionAny,
	}

	err := p.ProduceMany(context.Background(), []byte("order-key"), [][]byte{[]byte("a")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHooks(t *testing.T) {
	expectedErr := errors.New("produce failed")
	var produceCalls int
	var produceManyCalls int
	var produceFnCalls int

	mock := &mockClient{
		produceFn: func(_ *kafka.Message, deliveryChan chan kafka.Event) error {
			produceFnCalls++
			if produceFnCalls == 1 {
				deliveryChan <- &kafka.Message{}
				return nil
			}
			return expectedErr
		},
	}

	p := &Producer{
		client:    mock,
		topic:     "topic",
		partition: PartitionAny,
		hooks: Hooks{
			OnProduce: func(duration time.Duration, err error) {
				produceCalls++
				if duration < 0 {
					t.Fatal("expected non-negative duration")
				}
				if produceCalls == 1 && err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if produceCalls == 2 && !errors.Is(err, expectedErr) {
					t.Fatalf("unexpected error: %v", err)
				}
			},
			OnProduceMany: func(duration time.Duration, err error) {
				produceManyCalls++
				if duration < 0 {
					t.Fatal("expected non-negative duration")
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			},
		},
	}

	if err := p.Produce(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := p.Produce(context.Background(), []byte("k"), []byte("v")); err == nil {
		t.Fatal("expected error, got nil")
	}

	mock.produceFn = func(_ *kafka.Message, deliveryChan chan kafka.Event) error {
		deliveryChan <- &kafka.Message{}
		return nil
	}

	if err := p.ProduceMany(context.Background(), []byte("k"), nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := p.ProduceMany(context.Background(), []byte("k"), [][]byte{[]byte("a")}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if produceCalls != 2 {
		t.Fatalf("expected 2 produce hook calls, got %d", produceCalls)
	}
	if produceManyCalls != 2 {
		t.Fatalf("expected 2 produce many hook calls, got %d", produceManyCalls)
	}
}
