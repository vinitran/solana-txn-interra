package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"solana-txn-interra/config"
	"solana-txn-interra/models"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// KafkaProducer handles producing messages to Kafka
type KafkaProducer struct {
	producer     sarama.AsyncProducer
	syncProducer sarama.SyncProducer // Fallback for critical messages
	config       *config.Config
	successes    int64
	errors       int64
	closed       int32
}

// NewKafkaProducer creates a new async Kafka producer for high throughput
func NewKafkaProducer(cfg *config.Config) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal // Faster than WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Flush.Frequency = 10 * time.Millisecond // Flush every 10ms
	config.Producer.Flush.Messages = cfg.ProducerBatchSize
	config.Producer.Flush.Bytes = 1024 * 1024              // 1MB
	config.Producer.Compression = sarama.CompressionSnappy // Compress for better throughput

	// Configure SASL if provided
	if cfg.KafkaSecurityProtocol != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		if cfg.KafkaSASLUsername != "" {
			config.Net.SASL.User = cfg.KafkaSASLUsername
		}
		if cfg.KafkaSASLPassword != "" {
			config.Net.SASL.Password = cfg.KafkaSASLPassword
		}
	}

	// Create async producer
	asyncProducer, err := sarama.NewAsyncProducer(cfg.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	// Create sync producer as fallback
	syncConfig := *config
	syncConfig.Producer.RequiredAcks = sarama.WaitForAll
	syncProducer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, &syncConfig)
	if err != nil {
		asyncProducer.Close()
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	kp := &KafkaProducer{
		producer:     asyncProducer,
		syncProducer: syncProducer,
		config:       cfg,
	}

	// Start goroutines to handle successes and errors
	go kp.handleSuccesses()
	go kp.handleErrors()

	log.Printf("Async Kafka producer initialized with batch size: %d", cfg.ProducerBatchSize)
	return kp, nil
}

// PublishRollingStats publishes rolling statistics to Kafka asynchronously
func (kp *KafkaProducer) PublishRollingStats(stats *models.RollingStats) error {
	if atomic.LoadInt32(&kp.closed) == 1 {
		return fmt.Errorf("producer is closed")
	}

	jsonData, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("error marshaling rolling stats: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: kp.config.KafkaOutputTopic,
		Key:   sarama.StringEncoder(stats.PairID),
		Value: sarama.ByteEncoder(jsonData),
	}

	// Non-blocking send
	select {
	case kp.producer.Input() <- message:
		return nil
	case <-time.After(100 * time.Millisecond):
		return fmt.Errorf("producer queue full, message dropped")
	}
}

// PublishRollingStatsSync publishes rolling statistics synchronously (for critical messages)
func (kp *KafkaProducer) PublishRollingStatsSync(stats *models.RollingStats) error {
	jsonData, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("error marshaling rolling stats: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: kp.config.KafkaOutputTopic,
		Key:   sarama.StringEncoder(stats.PairID),
		Value: sarama.ByteEncoder(jsonData),
	}

	_, _, err = kp.syncProducer.SendMessage(message)
	return err
}

// handleSuccesses handles successful message deliveries
func (kp *KafkaProducer) handleSuccesses() {
	for msg := range kp.producer.Successes() {
		atomic.AddInt64(&kp.successes, 1)
		_ = msg // Can log if needed
	}
}

// handleErrors handles message delivery errors
func (kp *KafkaProducer) handleErrors() {
	for err := range kp.producer.Errors() {
		atomic.AddInt64(&kp.errors, 1)
		log.Printf("Producer error: %v", err.Err)
	}
}

// GetStats returns producer statistics
func (kp *KafkaProducer) GetStats() (successes, errors int64) {
	return atomic.LoadInt64(&kp.successes), atomic.LoadInt64(&kp.errors)
}

// Close closes the producer
func (kp *KafkaProducer) Close() error {
	atomic.StoreInt32(&kp.closed, 1)

	// Close async producer
	if err := kp.producer.Close(); err != nil {
		return err
	}

	// Close sync producer
	return kp.syncProducer.Close()
}
