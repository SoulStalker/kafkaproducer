package main

import (
	"context"
	"log"
	"time"

	"github.com/SoulStalker/kafkaproducer/internal/config"
	"github.com/SoulStalker/kafkaproducer/internal/producer"
)

func main() {
	cfg := config.MustLoad("config/prod.yml")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	kafkaConfig := producer.KafkaConfig{
		Brokers:      cfg.Kafka.Brokers,
		Topic:        cfg.Kafka.Topic,
		DLQTopic:     cfg.Kafka.DLQTopic,
		MaxRetries:   cfg.Kafka.MaxRetries,
		RetryBackoff: time.Duration(cfg.Kafka.RetryBackoffMs) * time.Millisecond,
		BatchTimeout: time.Duration(cfg.Kafka.BatchTimeoutMs) * time.Millisecond,
		Compression:  producer.GetCompression(cfg.Kafka.Compression),
		WriteTimeout: time.Second * 5,
	}

	p := producer.NewKafkaProducer(kafkaConfig)
	defer func() {
		if err := p.Close(); err != nil {
			log.Printf("[kafka] error closing producer: %v", err)
		}
	}()

	if err := p.SendUserCommand(ctx, producer.UserCommandMessage{
		ChatID:  246645984,
		Command: "TOTAL",
	}); err != nil {
		log.Printf("[main] failed to send command: %v", err)
	}
}
