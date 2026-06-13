package config

import (
	"fmt"
	"strings"

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

type Common struct {
	Hosts            []string
	Username         string
	Password         string
	SecurityProtocol SecurityProtocol
	SASLMechanism    SASLMechanism
	CaLocation       string
	CertLocation     string
	KeyLocation      string
}

func KafkaConfig(cfg Common) (kafka.ConfigMap, error) {
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

	if upperProtocol == "SSL" || upperProtocol == "SASL_SSL" {
		if cfg.CaLocation == "" {
			return nil, fmt.Errorf("CaLocation must be set for security protocol %s", protocol)
		}

		m["ssl.ca.location"] = cfg.CaLocation
		if cfg.CertLocation != "" {
			m["ssl.certificate.location"] = cfg.CertLocation
		}
		if cfg.KeyLocation != "" {
			m["ssl.key.location"] = cfg.KeyLocation
		}
	}

	return m, nil
}

func ApplyOverrides(target kafka.ConfigMap, overrides map[string]any) {
	for key, value := range overrides {
		target[key] = value
	}
}
