# kafkaproducer

Go-библиотека для отправки сообщений в Kafka с поддержкой retry и Dead Letter Queue (DLQ).

## Возможности

- Идемпотентная отправка через MD5-ключ (filename + payload)
- Автоматические повторные попытки с экспоненциальным backoff
- Пересылка в DLQ при исчерпании попыток
- Поддержка сжатия: gzip, snappy, lz4, zstd
- Синхронная запись с подтверждением от всех реплик (`RequireAll`)

## Конфигурация

Конфиг задаётся в YAML-файле. Путь передаётся в `config.MustLoad()`.

```yaml
kafka:
  brokers:
    - "localhost:9092"
  topic: "user_commands"
  dlq_topic: "user_commands_dlq"
  max_retries: 3
  retry_backoff_ms: 1000
  batch_timeout_ms: 100
  compression: "gzip"   # gzip | snappy | lz4 | zstd

app:
  poll_interval_sec: 10
  log_level: "info"
```

## Использование

```go
cfg := config.MustLoad("config/prod.yml")

p := producer.NewKafkaProducer(producer.KafkaConfig{
    Brokers:      cfg.Kafka.Brokers,
    Topic:        cfg.Kafka.Topic,
    DLQTopic:     cfg.Kafka.DLQTopic,
    MaxRetries:   cfg.Kafka.MaxRetries,
    RetryBackoff: time.Duration(cfg.Kafka.RetryBackoffMs) * time.Millisecond,
    BatchTimeout: time.Duration(cfg.Kafka.BatchTimeoutMs) * time.Millisecond,
    Compression:  producer.GetCompression(cfg.Kafka.Compression),
    WriteTimeout: time.Second * 5,
})
defer p.Close()

// Отправка команды пользователя
err := p.SendUserCommand(ctx, producer.UserCommandMessage{
    ChatID:  12345,
    Command: "TOTAL",
})

// Отправка произвольного сообщения
err := p.SendMessage(ctx, "filename.json", data)
```

## Структура

```
cmd/main.go                  — точка входа
internal/config/config.go    — загрузка YAML-конфига (cleanenv)
internal/producer/
    producer.go              — KafkaProducer: отправка, retry, DLQ
    interface.go             — интерфейс MessageProducer
config/
    prod.yml                 — продовый конфиг
    example.yml              — пример конфига
```

## Сборка и запуск

```bash
go build ./...
go run cmd/main.go
```
