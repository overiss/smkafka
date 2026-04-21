package smkafka

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func isAssignmentLostError(err error) bool {
	if err == nil {
		return false
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Code() == kafka.ErrAssignmentLost
	}

	return false
}
