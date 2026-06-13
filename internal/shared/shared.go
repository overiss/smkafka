package shared

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const DefaultReadinessTimeout = 3 * time.Second

func CallHook(hook func(time.Duration, error), start time.Time, err error) {
	if hook == nil {
		return
	}
	hook(time.Since(start), err)
}

func IsAssignmentLostError(err error) bool {
	if err == nil {
		return false
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Code() == kafka.ErrAssignmentLost
	}

	return false
}
