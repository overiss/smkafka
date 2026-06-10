package smkafka

import "testing"

func TestCommonKafkaConfigSASLSSLRequiresOnlyCaLocation(t *testing.T) {
	cfg, err := commonKafkaConfig(CommonConfig{
		Hosts:            []string{"localhost:9093"},
		SecurityProtocol: SecurityProtocolSASLSSL,
		Username:         "user",
		Password:         "pass",
		CaLocation:       "/etc/certs/ca.pem",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg["ssl.ca.location"] != "/etc/certs/ca.pem" {
		t.Fatalf("unexpected ssl.ca.location: %v", cfg["ssl.ca.location"])
	}
	if _, ok := cfg["ssl.certificate.location"]; ok {
		t.Fatal("expected ssl.certificate.location to be omitted")
	}
	if _, ok := cfg["ssl.key.location"]; ok {
		t.Fatal("expected ssl.key.location to be omitted")
	}
}

func TestCommonKafkaConfigSASLSSLWithClientCert(t *testing.T) {
	cfg, err := commonKafkaConfig(CommonConfig{
		Hosts:            []string{"localhost:9093"},
		SecurityProtocol: SecurityProtocolSASLSSL,
		Username:         "user",
		Password:         "pass",
		CaLocation:       "/etc/certs/ca.pem",
		CertLocation:     "/etc/certs/client.pem",
		KeyLocation:      "/etc/certs/client.key",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg["ssl.certificate.location"] != "/etc/certs/client.pem" {
		t.Fatalf("unexpected ssl.certificate.location: %v", cfg["ssl.certificate.location"])
	}
	if cfg["ssl.key.location"] != "/etc/certs/client.key" {
		t.Fatalf("unexpected ssl.key.location: %v", cfg["ssl.key.location"])
	}
}

func TestCommonKafkaConfigSSLRequiresCaLocation(t *testing.T) {
	_, err := commonKafkaConfig(CommonConfig{
		Hosts:            []string{"localhost:9093"},
		SecurityProtocol: SecurityProtocolSSL,
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestCommonKafkaConfigSSLWithCaLocation(t *testing.T) {
	cfg, err := commonKafkaConfig(CommonConfig{
		Hosts:            []string{"localhost:9093"},
		SecurityProtocol: SecurityProtocolSSL,
		CaLocation:       "/etc/certs/ca.pem",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg["ssl.ca.location"] != "/etc/certs/ca.pem" {
		t.Fatalf("unexpected ssl.ca.location: %v", cfg["ssl.ca.location"])
	}
}
