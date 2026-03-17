package producer

import "context"

// MessageProducer is a common interface for all message brokers.
type MessageProducer interface {
	SendMessage(ctx context.Context, fileName string, data []byte) error
	Close() error
}
