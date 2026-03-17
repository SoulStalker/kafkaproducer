package producer

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer    *kafka.Writer
	dlqWriter *kafka.Writer
	config    KafkaConfig
}

type KafkaConfig struct {
	Brokers      []string
	Topic        string
	DLQTopic     string
	MaxRetries   int
	RetryBackoff time.Duration
	BatchTimeout time.Duration
	Compression  kafka.Compression
	WriteTimeout time.Duration
}

func NewKafkaProducer(cfg KafkaConfig) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: cfg.BatchTimeout,
		Compression:  cfg.Compression,
		MaxAttempts:  cfg.MaxRetries,
		WriteTimeout: cfg.WriteTimeout,
		ReadTimeout:  cfg.WriteTimeout,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.DLQTopic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  1,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaProducer{
		writer:    writer,
		dlqWriter: dlqWriter,
		config:    cfg,
	}
}

type UserCommandMessage struct {
	ChatID  int64  `json:"chat_id"`
	Command string `json:"command"`
}

func (p *KafkaProducer) SendMessage(ctx context.Context, payload string, data []byte) error {
	idempotencyKey := p.generateIdempotencyKey(payload, data)

	message := kafka.Message{
		Key:   []byte(idempotencyKey),
		Value: data,
		Headers: []kafka.Header{
			{Key: "filename", Value: []byte(payload)},
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
			{Key: "idempotency-key", Value: []byte(idempotencyKey)},
		},
	}

	if err := p.writeWithRetry(ctx, message); err != nil {
		log.Printf("[kafka] failed to send message after %d retries: file=%s key=%s err=%v", p.config.MaxRetries, payload, idempotencyKey, err)
		if dlqErr := p.sendToDLQ(ctx, message, err); dlqErr != nil {
			return fmt.Errorf("failed to send to DLQ: %v (original: %w)", dlqErr, err)
		}
		return fmt.Errorf("message sent to DLQ after retries exhausted: %w", err)
	}

	log.Printf("[kafka] message sent: file=%s key=%s", payload, idempotencyKey)
	return nil
}

func (p *KafkaProducer) writeWithRetry(ctx context.Context, msg kafka.Message) error {
	var lastErr error
	for attempt := 0; attempt < p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := p.config.RetryBackoff * time.Duration(attempt)
			log.Printf("[kafka] retry %d/%d after %v: %v", attempt+1, p.config.MaxRetries, backoff, lastErr)
			time.Sleep(backoff)
		}

		if err := p.writer.WriteMessages(ctx, msg); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	return fmt.Errorf("all %d attempts failed: %w", p.config.MaxRetries, lastErr)
}

func (p *KafkaProducer) sendToDLQ(ctx context.Context, originalMsg kafka.Message, originalErr error) error {
	dlqMsg := kafka.Message{
		Key:   originalMsg.Key,
		Value: originalMsg.Value,
		Headers: append(originalMsg.Headers,
			kafka.Header{Key: "dlq-reason", Value: []byte(originalErr.Error())},
			kafka.Header{Key: "dlq-timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		),
	}

	if err := p.dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
		return fmt.Errorf("DLQ write failed for key=%s: %w", string(originalMsg.Key), err)
	}

	log.Printf("[kafka] message forwarded to DLQ: key=%s reason=%s", string(originalMsg.Key), originalErr.Error())
	return nil
}

func (p *KafkaProducer) generateIdempotencyKey(fileName string, data []byte) string {
	hash := md5.New()
	hash.Write([]byte(fileName))
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func (p *KafkaProducer) Close() error {
	if err := p.writer.Close(); err != nil {
		return err
	}
	return p.dlqWriter.Close()
}

func GetCompression(compression string) kafka.Compression {
	switch compression {
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return kafka.Snappy
	}
}

func (p *KafkaProducer) SendUserCommand(ctx context.Context, msg UserCommandMessage) error {
	if msg.ChatID == 0 {
		return fmt.Errorf("chat_id is required")
	}
	if msg.Command == "" {
		return fmt.Errorf("command is required")
	}

	payloadBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	idempotencyKey := p.generateIdempotencyKey(msg.Command, payloadBytes)

	message := kafka.Message{
		Key:   []byte(idempotencyKey),
		Value: payloadBytes,
		Headers: []kafka.Header{
			{Key: "event-type", Value: []byte("user_command")},
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
			{Key: "idempotency-key", Value: []byte(idempotencyKey)},
		},
	}

	if err = p.writeWithRetry(ctx, message); err != nil {
		log.Printf("[kafka] failed to send user command: chat_id=%d command=%s err=%v", msg.ChatID, msg.Command, err)
		if dlqErr := p.sendToDLQ(ctx, message, err); dlqErr != nil {
			return fmt.Errorf("failed to send to DLQ: %v (original: %w)", dlqErr, err)
		}
		return fmt.Errorf("message sent to DLQ after retries exhausted: %w", err)
	}

	log.Printf("[kafka] user command sent: chat_id=%d command=%s key=%s", msg.ChatID, msg.Command, idempotencyKey)
	return nil
}
