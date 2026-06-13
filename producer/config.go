package producer

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/overiss/smkafka/config"
)

type Hooks struct {
	OnProduce     func(duration time.Duration, err error)
	OnProduceMany func(duration time.Duration, err error)
}

type Config struct {
	Common           config.Common
	Name             string
	Topic            string
	Partition        *int32
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

	if cfg.ClientID != "" {
		result["client.id"] = cfg.ClientID
	}

	config.ApplyOverrides(result, cfg.Overrides)
	return result, nil
}
