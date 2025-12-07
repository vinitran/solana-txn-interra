package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"solana-txn-interra/config"
	"solana-txn-interra/models"

	"github.com/IBM/sarama"
)

// KafkaConsumer handles consuming messages from Kafka
type KafkaConsumer struct {
	consumer sarama.ConsumerGroup
	config   *config.Config
	handler  MessageHandler
}

// MessageHandler defines the interface for handling consumed messages
type MessageHandler interface {
	HandleTransaction(txn models.Transaction) error
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(cfg *config.Config, handler MessageHandler) (*KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

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

	consumer, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.KafkaConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return &KafkaConsumer{
		consumer: consumer,
		config:   cfg,
		handler:  handler,
	}, nil
}

// Start begins consuming messages
func (kc *KafkaConsumer) Start() error {
	handler := &consumerGroupHandler{
		handler: kc.handler,
	}

	log.Printf("Starting Kafka consumer for topic: %s", kc.config.KafkaInputTopic)

	ctx := context.Background()
	for {
		err := kc.consumer.Consume(ctx, []string{kc.config.KafkaInputTopic}, handler)
		if err != nil {
			return fmt.Errorf("error from consumer: %w", err)
		}
	}
}

// Close closes the consumer
func (kc *KafkaConsumer) Close() error {
	return kc.consumer.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	handler MessageHandler
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			var txn models.Transaction
			if err := json.Unmarshal(message.Value, &txn); err != nil {
				log.Printf("Error unmarshaling transaction: %v", err)
				session.MarkMessage(message, "")
				continue
			}

			if err := h.handler.HandleTransaction(txn); err != nil {
				log.Printf("Error handling transaction: %v", err)
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

