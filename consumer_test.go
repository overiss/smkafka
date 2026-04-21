package smkafka

import (
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type mockConsumerClient struct {
	commitFn          func() ([]kafka.TopicPartition, error)
	commitOffsetsFn   func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
	assignmentFn      func() ([]kafka.TopicPartition, error)
	offsetsForTimesFn func(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error)
	resumeFn          func(partitions []kafka.TopicPartition) error
}

func (m *mockConsumerClient) Poll(_ int) kafka.Event { return nil }

func (m *mockConsumerClient) Commit() ([]kafka.TopicPartition, error) {
	return m.commitFn()
}

func (m *mockConsumerClient) CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
	return m.commitOffsetsFn(offsets)
}

func (m *mockConsumerClient) Assignment() ([]kafka.TopicPartition, error) {
	return m.assignmentFn()
}

func (m *mockConsumerClient) OffsetsForTimes(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error) {
	return m.offsetsForTimesFn(times, timeoutMs)
}

func (m *mockConsumerClient) Resume(partitions []kafka.TopicPartition) error {
	return m.resumeFn(partitions)
}

func (m *mockConsumerClient) SubscribeTopics(_ []string, _ kafka.RebalanceCb) error { return nil }

func (m *mockConsumerClient) Close() error { return nil }

func TestConsumerCommitBatchReconnectRetry(t *testing.T) {
	topic := "topic"
	assignments := []kafka.TopicPartition{{Topic: &topic, Partition: 0}}
	commitOffsetsCalls := 0
	resumeCalls := 0

	mock := &mockConsumerClient{
		commitFn: func() ([]kafka.TopicPartition, error) { return nil, nil },
		commitOffsetsFn: func(_ []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
			commitOffsetsCalls++
			if commitOffsetsCalls == 1 {
				return nil, kafka.NewError(kafka.ErrAssignmentLost, "lost", false)
			}
			return []kafka.TopicPartition{{Topic: &topic, Partition: 0}}, nil
		},
		assignmentFn: func() ([]kafka.TopicPartition, error) {
			return assignments, nil
		},
		offsetsForTimesFn: func(times []kafka.TopicPartition, timeoutMs int) ([]kafka.TopicPartition, error) {
			if len(times) != 1 || timeoutMs <= 0 {
				t.Fatalf("unexpected reconnect params: len=%d timeout=%d", len(times), timeoutMs)
			}
			return times, nil
		},
		resumeFn: func(partitions []kafka.TopicPartition) error {
			resumeCalls++
			if len(partitions) != 1 {
				t.Fatalf("unexpected resume partitions len: %d", len(partitions))
			}
			return nil
		},
	}

	consumer := &Consumer{
		client:        mock,
		reconnectWait: time.Second,
	}
	consumer.setLastBatch([]*kafka.Message{
		{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: 0,
				Offset:    10,
			},
		},
	})

	err := consumer.CommitBatch()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if commitOffsetsCalls != 2 {
		t.Fatalf("expected 2 commit attempts, got %d", commitOffsetsCalls)
	}
	if resumeCalls != 1 {
		t.Fatalf("expected 1 reconnect resume, got %d", resumeCalls)
	}
	if batch := consumer.takeLastBatch(); len(batch) != 0 {
		t.Fatalf("expected batch to be cleared, got len=%d", len(batch))
	}
}

func TestConsumerCommitBatchRestoresBatchOnFailure(t *testing.T) {
	topic := "topic"
	expectedErr := errors.New("commit failed")
	mock := &mockConsumerClient{
		commitFn: func() ([]kafka.TopicPartition, error) { return nil, nil },
		commitOffsetsFn: func(_ []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
			return nil, expectedErr
		},
		assignmentFn:      func() ([]kafka.TopicPartition, error) { return nil, nil },
		offsetsForTimesFn: func(times []kafka.TopicPartition, _ int) ([]kafka.TopicPartition, error) { return times, nil },
		resumeFn:          func(_ []kafka.TopicPartition) error { return nil },
	}

	consumer := &Consumer{client: mock, reconnectWait: time.Second}
	consumer.setLastBatch([]*kafka.Message{
		{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: 1,
				Offset:    5,
			},
		},
	})

	err := consumer.CommitBatch()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if batch := consumer.takeLastBatch(); len(batch) != 1 {
		t.Fatalf("expected batch restored after error, got len=%d", len(batch))
	}
}
