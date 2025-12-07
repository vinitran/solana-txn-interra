package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"solana-txn-interra/config"
	"solana-txn-interra/models"

	"github.com/IBM/sarama"
)

// KafkaProducer handles producing messages to Kafka
type KafkaProducer struct {
	producer sarama.SyncProducer
	config   *config.Config
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(cfg *config.Config) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

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

	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &KafkaProducer{
		producer: producer,
		config:   cfg,
	}, nil
}

// PublishRollingStats publishes rolling statistics to Kafka
func (kp *KafkaProducer) PublishRollingStats(stats *models.RollingStats) error {
	jsonData, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("error marshaling rolling stats: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: kp.config.KafkaOutputTopic,
		Key:   sarama.StringEncoder(stats.PairID),
		Value: sarama.ByteEncoder(jsonData),
	}

	partition, offset, err := kp.producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("error sending message: %w", err)
	}

	log.Printf("Published rolling stats for pair %s to partition %d at offset %d", stats.PairID, partition, offset)
	return nil
}

// Close closes the producer
func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}

