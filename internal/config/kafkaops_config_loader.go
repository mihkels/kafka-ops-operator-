package config

import (
	"os"
	"strconv"
)

const (
	KeyKafkaClusterName = "KAFKA_CLUSTER_NAME"
	KeyKafkaPort        = "KAFKA_PORT"
	KeyKafkaAuth        = "KAFKA_AUTH"
)

type Config struct {
	KafkaClusterName string
	KafkaPort        string
	KafkaAuth        bool
}

func NewConfig() *Config {
	clusterName := os.Getenv(KeyKafkaClusterName)
	port := os.Getenv(KeyKafkaPort)
	kafkaAuth, err := strconv.ParseBool(os.Getenv(KeyKafkaAuth))
	if err != nil {
		kafkaAuth = false
	}

	return &Config{
		KafkaClusterName: clusterName,
		KafkaPort:        port,
		KafkaAuth:        kafkaAuth,
	}
}
