package consumer

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/overiss/smkafka/config"
)

type Hooks struct {
	OnCommit       func(duration time.Duration, err error)
	OnConsume      func(duration time.Duration, err error)
	OnConsumeBatch func(duration time.Duration, err error)
}

type Config struct {
	Common           config.Common
	Name             string
	Topic            string
	GroupID          string
	AutoOffsetReset  string
	EnableAutoCommit *bool
	BatchSize        int
	BatchDeadline    time.Duration
	ReconnectTimeout time.Duration
	ReadinessTimeout time.Duration
	ClientID         string
	Hooks            Hooks
	Overrides        map[string]any
}

func kafkaConfig(cfg Config) (kafka.ConfigMap, error) {
	result, err := config.KafkaConfig(cfg.Common)
	if err != nil {
		return nil, err
	}

	if cfg.GroupID == "" {
		return nil, fmt.Errorf("group id must not be empty")
	}
	result["group.id"] = cfg.GroupID

	if cfg.AutoOffsetReset != "" {
		result["auto.offset.reset"] = cfg.AutoOffsetReset
	}

	if cfg.EnableAutoCommit != nil {
		result["enable.auto.commit"] = *cfg.EnableAutoCommit
	} else {
		result["enable.auto.commit"] = false
	}

	if cfg.ClientID != "" {
		result["client.id"] = cfg.ClientID
	}

	config.ApplyOverrides(result, cfg.Overrides)
	return result, nil
}
