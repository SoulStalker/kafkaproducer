package config

import (
	"log"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Kafka KafkaConfig `yaml:"kafka"`
	App   AppConfig   `yaml:"app"`
}

type KafkaConfig struct {
	Brokers        []string `yaml:"brokers" env-required:"true"`
	Topic          string   `yaml:"topic" env-required:"true"`
	DLQTopic       string   `yaml:"dlq_topic"`
	MaxRetries     int      `yaml:"max_retries" env-default:"3"`
	RetryBackoffMs int      `yaml:"retry_backoff_ms" env-default:"1000"`
	BatchTimeoutMs int      `yaml:"batch_timeout_ms" env-default:"100"`
	Compression    string   `yaml:"compression" env-default:"gzip"`
}

type AppConfig struct {
	PollIntervalSec int    `yaml:"poll_interval_sec" env-default:"10"`
	LogLevel        string `yaml:"log_level" env-default:"info"`
}

func MustLoad(configPath string) *Config {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file does not exits: %s", configPath)
	}

	var cfg Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cannot read config: %s", err)
	}

	return &cfg
}
