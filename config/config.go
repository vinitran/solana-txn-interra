package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds application configuration
type Config struct {
	KafkaBrokers          []string
	KafkaConsumerGroup    string
	KafkaInputTopic       string
	KafkaOutputTopic      string
	KafkaSecurityProtocol string
	KafkaSASLMechanism    string
	KafkaSASLUsername     string
	KafkaSASLPassword     string
	RedisAddr             string
	RedisPassword         string
	RedisDB               int
	RedisEnabled          bool
	LocalCacheSize        int
	LocalCacheTTL         int // seconds
	WorkerPoolSize        int // Number of worker goroutines
	ProducerBatchSize     int // Batch size for async producer
	ConsumerFetchSize     int // Kafka consumer fetch size
}

// LoadConfig loads configuration from environment variables and .env file
func LoadConfig() *Config {
	// Try to load .env file (ignore error if file doesn't exist)
	if err := godotenv.Load(); err != nil {
		log.Printf("Info: .env file not found or cannot be loaded: %v", err)
	}

	// Also try .env.local (for local overrides)
	_ = godotenv.Overload(".env.local")

	// Parse Kafka brokers (support comma-separated list)
	// Support both KAFKA_BROKERS and KAFKA_BOOTSTRAP_SERVERS (for compatibility)
	brokersStr := getEnv("KAFKA_BROKERS", "")
	if brokersStr == "" {
		brokersStr = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	}
	brokers := strings.Split(brokersStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "transaction-processor-group")
	inputTopic := getEnv("KAFKA_INPUT_TOPIC", "transaction-v3")
	outputTopic := getEnv("KAFKA_OUTPUT_TOPIC", "rolling-stats-data")

	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	redisDB := getEnvAsInt("REDIS_DB", 0)
	redisEnabled := getEnvAsBool("REDIS_ENABLED", true)

	localCacheSize := getEnvAsInt("LOCAL_CACHE_SIZE", 10000)
	localCacheTTL := getEnvAsInt("LOCAL_CACHE_TTL", 60)
	workerPoolSize := getEnvAsInt("WORKER_POOL_SIZE", 100)             // Default 100 workers
	producerBatchSize := getEnvAsInt("PRODUCER_BATCH_SIZE", 100)       // Default batch 100
	consumerFetchSize := getEnvAsInt("CONSUMER_FETCH_SIZE", 1024*1024) // Default 1MB

	return &Config{
		KafkaBrokers:          brokers,
		KafkaConsumerGroup:    consumerGroup,
		KafkaInputTopic:       inputTopic,
		KafkaOutputTopic:      outputTopic,
		KafkaSecurityProtocol: getEnv("KAFKA_SECURITY_PROTOCOL", ""),
		KafkaSASLMechanism:    getEnv("KAFKA_SASL_MECHANISM", ""),
		KafkaSASLUsername:     getEnv("KAFKA_SASL_USERNAME", ""),
		KafkaSASLPassword:     getEnv("KAFKA_SASL_PASSWORD", ""),
		RedisAddr:             redisAddr,
		RedisPassword:         redisPassword,
		RedisDB:               redisDB,
		RedisEnabled:          redisEnabled,
		LocalCacheSize:        localCacheSize,
		LocalCacheTTL:         localCacheTTL,
		WorkerPoolSize:        workerPoolSize,
		ProducerBatchSize:     producerBatchSize,
		ConsumerFetchSize:     consumerFetchSize,
	}
}

// getEnv gets an environment variable or returns default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// getEnvAsInt gets an environment variable as integer or returns default value
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Printf("Warning: Invalid integer value for %s: %s, using default: %d", key, valueStr, defaultValue)
		return defaultValue
	}

	return value
}

// getEnvAsBool gets an environment variable as boolean or returns default value
// Accepts: "true", "1", "yes", "on" (case insensitive) for true
//
//	"false", "0", "no", "off", "" (case insensitive) for false
func getEnvAsBool(key string, defaultValue bool) bool {
	valueStr := strings.ToLower(strings.TrimSpace(os.Getenv(key)))

	if valueStr == "" {
		return defaultValue
	}

	switch valueStr {
	case "true", "1", "yes", "on":
		return true
	case "false", "0", "no", "off":
		return false
	default:
		log.Printf("Warning: Invalid boolean value for %s: %s, using default: %v", key, valueStr, defaultValue)
		return defaultValue
	}
}

// PrintConfig prints the configuration (without sensitive data)
func (c *Config) PrintConfig() {
	log.Printf("Configuration:")
	log.Printf("  Kafka Brokers: %v", c.KafkaBrokers)
	log.Printf("  Kafka Consumer Group: %s", c.KafkaConsumerGroup)
	log.Printf("  Kafka Input Topic: %s", c.KafkaInputTopic)
	log.Printf("  Kafka Output Topic: %s", c.KafkaOutputTopic)
	if c.KafkaSecurityProtocol != "" {
		log.Printf("  Kafka Security Protocol: %s", c.KafkaSecurityProtocol)
		log.Printf("  Kafka SASL Mechanism: %s", c.KafkaSASLMechanism)
		log.Printf("  Kafka SASL Username: %s", c.KafkaSASLUsername)
		log.Printf("  Kafka SASL Password: [REDACTED]")
	}
	log.Printf("  Redis Addr: %s", c.RedisAddr)
	log.Printf("  Redis DB: %d", c.RedisDB)
	log.Printf("  Redis Enabled: %v", c.RedisEnabled)
	if c.RedisPassword != "" {
		log.Printf("  Redis Password: [REDACTED]")
	}
	log.Printf("  Local Cache Size: %d", c.LocalCacheSize)
	log.Printf("  Local Cache TTL: %d seconds", c.LocalCacheTTL)
	log.Printf("  Worker Pool Size: %d", c.WorkerPoolSize)
	log.Printf("  Producer Batch Size: %d", c.ProducerBatchSize)
	log.Printf("  Consumer Fetch Size: %d bytes", c.ConsumerFetchSize)
}
