# Solana Transaction Processor

Golang application để xử lý transaction từ Kafka, tính toán rolling statistics và publish kết quả.

## Tính năng

- Nhận transaction từ Kafka topic `transaction-v3`
- Tính toán rolling statistics cho các timeframes: 1M, 5M, 6H, 24H
- Publish kết quả lên Kafka topic `rolling-stats-data`
- **Redis cache** để tối ưu hiệu năng cho 50k+ TPS
- **Local LRU cache** cho hot data để giảm latency
- **In-memory buffers** cho fast access
- Background cleanup để tự động dọn dẹp dữ liệu cũ

## Cấu trúc Project

```
.
├── main.go                 # Entry point
├── config/
│   └── config.go          # Configuration management
├── models/
│   ├── transaction.go     # Transaction input model
│   └── rolling_stats.go   # Rolling stats output model
├── consumer/
│   └── kafka_consumer.go  # Kafka consumer implementation
├── processor/
│   ├── processor.go          # Processor interface
│   ├── stats_processor.go    # Statistics calculation logic (basic)
│   └── cached_processor.go   # Cached processor with Redis optimization
├── cache/
│   ├── redis_cache.go         # Redis cache implementation
│   └── local_cache.go         # Local LRU cache
└── producer/
    └── kafka_producer.go      # Kafka producer implementation
```

## Cài đặt

1. Clone repository:
```bash
git clone <repository-url>
cd solana-txn-interra
```

2. Download dependencies:
```bash
go mod download
```

## Cấu hình

Cấu hình có thể được đặt qua:
1. **File `.env`** (ưu tiên) - Tạo file `.env` trong thư mục gốc
2. **Environment variables** - Đặt trực tiếp trong shell

### Tạo file .env

Tạo file `.env` trong thư mục gốc với nội dung:

```bash
# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CONSUMER_GROUP=transaction-processor-group
KAFKA_INPUT_TOPIC=transaction-v3
KAFKA_OUTPUT_TOPIC=rolling-stats-data

# Kafka Security (optional)
# KAFKA_SECURITY_PROTOCOL=SASL_SSL
# KAFKA_SASL_MECHANISM=PLAIN
# KAFKA_SASL_USERNAME=your_username
# KAFKA_SASL_PASSWORD=your_password

# Redis Configuration
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
REDIS_ENABLED=true

# Local Cache Configuration
LOCAL_CACHE_SIZE=10000
LOCAL_CACHE_TTL=60
```

### Environment Variables

Các biến môi trường có thể được đặt:

- `KAFKA_BROKERS`: Kafka broker addresses (default: `localhost:9092`)
- `KAFKA_CONSUMER_GROUP`: Consumer group name (default: `transaction-processor-group`)
- `KAFKA_INPUT_TOPIC`: Input topic name (default: `transaction-v3`)
- `KAFKA_OUTPUT_TOPIC`: Output topic name (default: `rolling-stats-data`)
- `KAFKA_SECURITY_PROTOCOL`: Security protocol (optional)
- `KAFKA_SASL_MECHANISM`: SASL mechanism (optional)
- `KAFKA_SASL_USERNAME`: SASL username (optional)
- `KAFKA_SASL_PASSWORD`: SASL password (optional)
- `REDIS_ADDR`: Redis server address (default: `localhost:6379`)
- `REDIS_PASSWORD`: Redis password (optional)
- `REDIS_DB`: Redis database number (default: `0`)
- `REDIS_ENABLED`: Enable Redis cache (default: `true`, set to `false` to disable)
- `LOCAL_CACHE_SIZE`: Local cache size in entries (default: `10000`)
- `LOCAL_CACHE_TTL`: Local cache TTL in seconds (default: `60`)

## Chạy ứng dụng

```bash
go run main.go
```

Hoặc build và chạy:

```bash
go build -o transaction-processor
./transaction-processor
```

## Cách hoạt động

1. Consumer đọc transaction từ topic `transaction-v3`
2. Processor tính toán rolling statistics cho mỗi transaction:
   - **Với Redis enabled**: Sử dụng hybrid cache (local + Redis)
     - Local LRU cache cho hot data (giảm latency)
     - Redis sorted sets để lưu transactions theo timestamp
     - In-memory buffers cho fast access
     - Async writes to Redis để không block processing
   - **Không Redis**: Sử dụng in-memory storage thuần
   - Tính toán: Tổng số transaction, buy, sell, volume, distinct makers, price change
3. Producer publish kết quả lên topic `rolling-stats-data`
4. Background cleanup tự động dọn dẹp dữ liệu cũ mỗi 30 giây

## Performance Optimization

Để xử lý 50k+ TPS, hệ thống sử dụng:

1. **Redis Sorted Sets**: Lưu transactions theo timestamp để query nhanh theo time range
2. **Local LRU Cache**: Cache aggregated stats để tránh tính toán lại
3. **In-Memory Buffers**: Giữ recent transactions trong memory cho fast access
4. **Async Operations**: Redis writes không block main processing thread
5. **Connection Pooling**: Redis connection pool (100 connections) cho high throughput
6. **Batch Cleanup**: Background goroutine cleanup dữ liệu cũ

### Redis Key Structure

- `txns:{pair_id}:{timeframe}` - Sorted set chứa transactions (score = timestamp)
- `stats:{pair_id}:{timeframe}` - Hash chứa aggregated statistics (cached)

### Ví dụ cấu hình cho production:

```bash
export REDIS_ADDR=redis-cluster:6379
export REDIS_PASSWORD=your_password
export REDIS_ENABLED=true
export LOCAL_CACHE_SIZE=50000
export LOCAL_CACHE_TTL=30
```

## Rolling Statistics

Ứng dụng tính toán các metrics sau cho mỗi timeframe (1M, 5M, 6H, 24H):

- `txn_count`: Tổng số transaction
- `txn_buy_count`: Số lượng buy transaction
- `txn_sell_count`: Số lượng sell transaction
- `total_volume`: Tổng volume (USD)
- `total_buy_volume`: Tổng buy volume (USD)
- `total_sell_volume`: Tổng sell volume (USD)
- `distinct_makers`: Số lượng distinct makers
- `distinct_buy_makers`: Số lượng distinct buy makers
- `distinct_sell_makers`: Số lượng distinct sell makers
- `price_pct_change`: Thay đổi giá phần trăm so với open price đầu kỳ

## Performance Tuning

### Để đạt 50k+ TPS:

1. **Redis**: Sử dụng Redis cluster hoặc Redis với persistence disabled cho maximum performance
2. **Local Cache**: Tăng `LOCAL_CACHE_SIZE` nếu có nhiều RAM (ví dụ: 50000-100000)
3. **Kafka**: Tối ưu Kafka consumer settings (batch size, fetch size)
4. **Go Runtime**: Tăng `GOMAXPROCS` nếu có nhiều CPU cores

### Monitoring:

- Monitor Redis memory usage
- Monitor local cache hit rate
- Monitor processing latency
- Monitor Kafka consumer lag

## License

MIT

