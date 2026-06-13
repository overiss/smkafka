// Package smkafka provides a simplified Kafka client API built on confluent-kafka-go.
//
// Subpackages producer and consumer contain the main client implementations.
// This root package re-exports their public API for backward compatibility.
package smkafka

import (
	"github.com/overiss/smkafka/config"
	"github.com/overiss/smkafka/consumer"
	"github.com/overiss/smkafka/producer"
)

type CommonConfig = config.Common

type SecurityProtocol = config.SecurityProtocol

const (
	SecurityProtocolPlaintext = config.SecurityProtocolPlaintext
	SecurityProtocolSSL       = config.SecurityProtocolSSL
	SecurityProtocolSASLPlain = config.SecurityProtocolSASLPlain
	SecurityProtocolSASLSSL   = config.SecurityProtocolSASLSSL
)

type SASLMechanism = config.SASLMechanism

const (
	SASLMechanismPlain       = config.SASLMechanismPlain
	SASLMechanismSCRAMSHA256 = config.SASLMechanismSCRAMSHA256
	SASLMechanismSCRAMSHA512 = config.SASLMechanismSCRAMSHA512
)

type Producer = producer.Producer

type ProducerConfig = producer.Config

type ProducerHooks = producer.Hooks

const PartitionAny = producer.PartitionAny

var NewProducer = producer.New

type Consumer = consumer.Consumer

type ConsumerConfig = consumer.Config

type ConsumerHooks = consumer.Hooks

type Message = consumer.Message

type Header = consumer.Header

var NewConsumer = consumer.New
