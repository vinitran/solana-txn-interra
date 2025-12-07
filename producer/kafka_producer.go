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
	dropped      int64 // Messages dropped due to queue full
	closed       int32
}

// NewKafkaProducer creates a new async Kafka producer optimized for realtime
func NewKafkaProducer(cfg *config.Config) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForLocal // Fastest: don't wait for all replicas
	config.Producer.Retry.Max = 2                      // Reduce retries for lower latency
	config.Producer.Timeout = 5 * time.Second          // Shorter timeout
	config.ChannelBufferSize = cfg.ProducerBufferSize  // Large buffer to prevent queue full

	// Optimize for realtime mode
	if cfg.RealtimeMode {
		// Realtime: flush immediately, no batching
		config.Producer.Flush.Frequency = time.Duration(cfg.ProducerFlushInterval) * time.Millisecond
		config.Producer.Flush.Messages = 1                   // Flush after 1 message
		config.Producer.Flush.Bytes = 0                      // No byte-based batching
		config.Producer.Compression = sarama.CompressionNone // No compression for lower latency
		config.Producer.MaxMessageBytes = 1000000            // 1MB max message size
		log.Printf("Producer configured for REALTIME mode (flush: %dms, no batching, no compression)", cfg.ProducerFlushInterval)
	} else {
		// Throughput mode: batch for better throughput
		config.Producer.Flush.Frequency = 10 * time.Millisecond
		config.Producer.Flush.Messages = cfg.ProducerBatchSize
		config.Producer.Flush.Bytes = 1024 * 1024 // 1MB
		config.Producer.Compression = sarama.CompressionSnappy
		log.Printf("Producer configured for THROUGHPUT mode (batch: %d, compression enabled)", cfg.ProducerBatchSize)
	}

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

	return kp, nil
}

// PublishRollingStats publishes rolling statistics to Kafka asynchronously with retry
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
		// Set timestamp for better ordering
		Timestamp: time.Now(),
	}

	// Retry logic with exponential backoff
	maxRetries := kp.config.ProducerMaxRetries
	baseTimeout := 1 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case kp.producer.Input() <- message:
			return nil
		case <-time.After(baseTimeout * time.Duration(1<<uint(attempt))):
			// Exponential backoff: 1ms, 2ms, 4ms, 8ms...
			if attempt == maxRetries-1 {
				// Last attempt failed, try sync producer as fallback
				atomic.AddInt64(&kp.dropped, 1)
				return kp.publishWithSyncFallback(stats, jsonData)
			}
			// Continue to next retry
		}
	}

	// Should not reach here, but fallback to sync
	atomic.AddInt64(&kp.dropped, 1)
	return kp.publishWithSyncFallback(stats, jsonData)
}

// publishWithSyncFallback publishes using sync producer when async queue is full
func (kp *KafkaProducer) publishWithSyncFallback(stats *models.RollingStats, jsonData []byte) error {
	message := &sarama.ProducerMessage{
		Topic:     kp.config.KafkaOutputTopic,
		Key:       sarama.StringEncoder(stats.PairID),
		Value:     sarama.ByteEncoder(jsonData),
		Timestamp: time.Now(),
	}

	_, _, err := kp.syncProducer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("sync fallback also failed: %w", err)
	}
	return nil
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
func (kp *KafkaProducer) GetStats() (successes, errors, dropped int64) {
	return atomic.LoadInt64(&kp.successes), atomic.LoadInt64(&kp.errors), atomic.LoadInt64(&kp.dropped)
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
