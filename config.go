package smkafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type SecurityProtocol string

const (
	SecurityProtocolPlaintext SecurityProtocol = "PLAINTEXT"
	SecurityProtocolSSL       SecurityProtocol = "SSL"
	SecurityProtocolSASLPlain SecurityProtocol = "SASL_PLAINTEXT"
	SecurityProtocolSASLSSL   SecurityProtocol = "SASL_SSL"
)

type SASLMechanism string

const (
	SASLMechanismPlain       SASLMechanism = "PLAIN"
	SASLMechanismSCRAMSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLMechanismSCRAMSHA512 SASLMechanism = "SCRAM-SHA-512"
)

type CommonConfig struct {
	Hosts            []string
	Username         string
	Password         string
	SecurityProtocol SecurityProtocol
	SASLMechanism    SASLMechanism
	CaLocation       string
	CertLocation     string
	KeyLocation      string
}

type ProducerConfig struct {
	Common    CommonConfig
	Topic     string
	Partition *int32
	ClientID  string
	Overrides map[string]any
}

type ConsumerConfig struct {
	Common           CommonConfig
	Topic            string
	GroupID          string
	AutoOffsetReset  string
	EnableAutoCommit *bool
	BatchSize        int
	BatchDeadline    time.Duration
	ReconnectTimeout time.Duration
	ClientID         string
	Overrides        map[string]any
}

func producerKafkaConfig(cfg ProducerConfig) (kafka.ConfigMap, error) {
	result, err := commonKafkaConfig(cfg.Common)
	if err != nil {
		return nil, err
	}

	if cfg.ClientID != "" {
		result["client.id"] = cfg.ClientID
	}

	applyOverrides(result, cfg.Overrides)
	return result, nil
}

func consumerKafkaConfig(cfg ConsumerConfig) (kafka.ConfigMap, error) {
	result, err := commonKafkaConfig(cfg.Common)
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
		// Library exposes explicit commit methods, so auto commit is disabled by default.
		result["enable.auto.commit"] = false
	}

	if cfg.ClientID != "" {
		result["client.id"] = cfg.ClientID
	}

	applyOverrides(result, cfg.Overrides)
	return result, nil
}

func commonKafkaConfig(cfg CommonConfig) (kafka.ConfigMap, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("hosts must not be empty")
	}

	m := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.Hosts, ","),
	}

	protocol := string(cfg.SecurityProtocol)
	if protocol == "" {
		protocol = string(SecurityProtocolPlaintext)
	}
	m["security.protocol"] = protocol

	upperProtocol := strings.ToUpper(protocol)
	if strings.HasPrefix(upperProtocol, "SASL_") {
		if cfg.Username == "" || cfg.Password == "" {
			return nil, fmt.Errorf("username and password must be set for security protocol %s", protocol)
		}

		m["sasl.username"] = cfg.Username
		m["sasl.password"] = cfg.Password

		mechanism := string(cfg.SASLMechanism)
		if mechanism == "" {
			mechanism = string(SASLMechanismPlain)
		}
		m["sasl.mechanism"] = mechanism
	}

	if upperProtocol == "SASL_SSL" {
		if cfg.CaLocation == "" || cfg.CertLocation == "" {
			return nil, fmt.Errorf("CaLocation [%s] or CertLocation [%s] is not set", cfg.CaLocation, cfg.CertLocation)
		}

		m["ssl.certificate.location"] = cfg.CertLocation
		m["ssl.ca.location"] = cfg.CaLocation
		if cfg.KeyLocation != "" {
			m["ssl.key.location"] = cfg.KeyLocation
		}
	}

	return m, nil
}

func applyOverrides(target kafka.ConfigMap, overrides map[string]any) {
	for key, value := range overrides {
		target[key] = value
	}
}
