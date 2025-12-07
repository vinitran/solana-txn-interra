package processor

import (
	"solana-txn-interra/cache"
	"solana-txn-interra/models"
	"sync"
	"time"

	"github.com/google/uuid"
)

// CachedStatsProcessor processes transactions with Redis and local cache optimization
type CachedStatsProcessor struct {
	redisCache   *cache.RedisCache
	localCache   *cache.LocalCache
	buffers      map[string]map[string]*cache.TransactionBuffer // pair_id -> timeframe -> buffer
	bufferMu     sync.RWMutex
	cleanupTicker *time.Ticker
	stopCleanup   chan bool
}

// NewCachedStatsProcessor creates a new cached stats processor
func NewCachedStatsProcessor(redisCache *cache.RedisCache, localCache *cache.LocalCache) *CachedStatsProcessor {
	processor := &CachedStatsProcessor{
		redisCache: redisCache,
		localCache: localCache,
		buffers:    make(map[string]map[string]*cache.TransactionBuffer),
		stopCleanup: make(chan bool),
	}

	// Start background cleanup goroutine
	processor.startBackgroundCleanup()

	return processor
}

// ProcessTransaction processes a transaction and calculates rolling statistics with caching
func (csp *CachedStatsProcessor) ProcessTransaction(txn models.Transaction) *models.RollingStats {
	now := time.Now()
	pairID := txn.PairID

	// Define timeframes
	timeframes := []struct {
		key      string
		duration time.Duration
	}{
		{"1m", 1 * time.Minute},
		{"5m", 5 * time.Minute},
		{"6h", 6 * time.Hour},
		{"24h", 24 * time.Hour},
	}

	// Update stats for each timeframe (async to Redis to avoid blocking)
	for _, tf := range timeframes {
		csp.updateTimeframeStatsAsync(pairID, tf.key, txn, now, tf.duration)
	}

	// Calculate and return rolling stats (using cached data when possible)
	return csp.calculateRollingStats(pairID, txn, now, timeframes)
}

// updateTimeframeStatsAsync updates statistics asynchronously
func (csp *CachedStatsProcessor) updateTimeframeStatsAsync(pairID, timeframe string, txn models.Transaction, now time.Time, duration time.Duration) {
	// Get or create buffer
	csp.bufferMu.Lock()
	if csp.buffers[pairID] == nil {
		csp.buffers[pairID] = make(map[string]*cache.TransactionBuffer)
	}
	buffer, exists := csp.buffers[pairID][timeframe]
	if !exists {
		buffer = cache.NewTransactionBuffer(10000) // Keep last 10k transactions in memory
		csp.buffers[pairID][timeframe] = buffer
	}
	csp.bufferMu.Unlock()

	// Add to local buffer
	buffer.Add(txn)

	// Add to Redis (async, don't block)
	go func() {
		if csp.redisCache != nil {
			csp.redisCache.AddTransaction(pairID, timeframe, txn)
		}
	}()

	// Invalidate local cache for this pair/timeframe
	csp.localCache.InvalidateStats(pairID, timeframe)
}

// calculateRollingStats calculates rolling statistics using cached data
func (csp *CachedStatsProcessor) calculateRollingStats(pairID string, latestTxn models.Transaction, now time.Time, timeframes []struct {
	key      string
	duration time.Duration
}) *models.RollingStats {
	stats := &models.RollingStats{
		PairID:             pairID,
		Timestamp:          now,
		RunID:              uuid.New().String(),
		LiquiditySOL:       latestTxn.LiquiditySOL,
		LiquidityUSD:       latestTxn.LiquidityUSD,
		PriceSOL:           latestTxn.PriceSOL,
		PriceUSD:           latestTxn.PriceUSD,
		ProcessedTimestamp: now,
	}

	// Calculate stats for each timeframe
	for _, tf := range timeframes {
		csp.calculateTimeframeStats(pairID, tf.key, latestTxn, now, tf.duration, stats)
	}

	return stats
}

// calculateTimeframeStats calculates statistics for a specific timeframe
func (csp *CachedStatsProcessor) calculateTimeframeStats(pairID, timeframe string, latestTxn models.Transaction, now time.Time, duration time.Duration, stats *models.RollingStats) {
	cutoffTime := now.Add(-duration)

	// Try to get from local cache first
	if cachedStats, ok := csp.localCache.GetStats(pairID, timeframe); ok {
		// Check if cache is still valid (within last 5 seconds)
		if time.Since(cachedStats.LastUpdated) < 5*time.Second {
			csp.applyCachedStats(timeframe, cachedStats, latestTxn, stats)
			return
		}
	}

	// Get transactions from buffer (fast path for recent data)
	csp.bufferMu.RLock()
	buffer, exists := csp.buffers[pairID][timeframe]
	csp.bufferMu.RUnlock()

	var transactions []models.Transaction
	if exists {
		transactions = buffer.GetRecent(cutoffTime)
	}

	// If buffer doesn't have enough data, try Redis
	if len(transactions) == 0 && csp.redisCache != nil {
		redisTxns, err := csp.redisCache.GetTransactionsInRange(pairID, timeframe, cutoffTime, now)
		if err == nil && len(redisTxns) > 0 {
			transactions = redisTxns
		}
	}

	// Calculate stats from transactions
	aggregated := csp.aggregateTransactions(transactions, cutoffTime)

	// Cache the results
	csp.localCache.SetStats(pairID, timeframe, aggregated)

	// Apply to rolling stats
	csp.applyAggregatedStats(timeframe, aggregated, latestTxn, stats)
}

// aggregateTransactions aggregates transactions into statistics
func (csp *CachedStatsProcessor) aggregateTransactions(transactions []models.Transaction, cutoffTime time.Time) *cache.AggregatedStats {
	stats := &cache.AggregatedStats{
		BuyMakers:   make([]string, 0),
		SellMakers:  make([]string, 0),
		LastUpdated: time.Now(),
	}

	buyMakerSet := make(map[string]bool)
	sellMakerSet := make(map[string]bool)
	var oldestTxn *models.Transaction

	for _, txn := range transactions {
		if txn.Timestamp.Before(cutoffTime) {
			continue
		}

		stats.TotalVolume += txn.AmountUSD

		if txn.Type == "buy" {
			stats.BuyCount++
			stats.BuyVolume += txn.AmountUSD
			if !buyMakerSet[txn.Maker] {
				buyMakerSet[txn.Maker] = true
				stats.BuyMakers = append(stats.BuyMakers, txn.Maker)
			}
		} else if txn.Type == "sell" {
			stats.SellCount++
			stats.SellVolume += txn.AmountUSD
			if !sellMakerSet[txn.Maker] {
				sellMakerSet[txn.Maker] = true
				stats.SellMakers = append(stats.SellMakers, txn.Maker)
			}
		}

		// Track oldest transaction for open price
		if oldestTxn == nil || txn.Timestamp.Before(oldestTxn.Timestamp) {
			oldestTxn = &txn
		}
	}

	if oldestTxn != nil {
		stats.OpenPrice = oldestTxn.PriceUSD
	}

	return stats
}

// applyCachedStats applies cached statistics to rolling stats
func (csp *CachedStatsProcessor) applyCachedStats(timeframe string, cached *cache.AggregatedStats, latestTxn models.Transaction, stats *models.RollingStats) {
	var pricePctChange float64
	if cached.OpenPrice > 0 {
		pricePctChange = ((latestTxn.PriceUSD - cached.OpenPrice) / cached.OpenPrice) * 100
	}

	switch timeframe {
	case "1m":
		stats.Last1MTxnCount = cached.BuyCount + cached.SellCount
		stats.Last1MTxnBuyCount = cached.BuyCount
		stats.Last1MTxnSellCount = cached.SellCount
		stats.Last1MTotalVolume = cached.TotalVolume
		stats.Last1MTotalBuyVolume = cached.BuyVolume
		stats.Last1MTotalSellVolume = cached.SellVolume
		stats.Last1MDistinctMakers = len(cached.BuyMakers) + len(cached.SellMakers) - csp.countOverlap(cached.BuyMakers, cached.SellMakers)
		stats.Last1MDistinctBuyMakers = len(cached.BuyMakers)
		stats.Last1MDistinctSellMakers = len(cached.SellMakers)
		stats.Last1MPricePctChange = pricePctChange
	case "5m":
		stats.Last5MTxnCount = cached.BuyCount + cached.SellCount
		stats.Last5MTxnBuyCount = cached.BuyCount
		stats.Last5MTxnSellCount = cached.SellCount
		stats.Last5MTotalVolume = cached.TotalVolume
		stats.Last5MTotalBuyVolume = cached.BuyVolume
		stats.Last5MTotalSellVolume = cached.SellVolume
		stats.Last5MDistinctMakers = len(cached.BuyMakers) + len(cached.SellMakers) - csp.countOverlap(cached.BuyMakers, cached.SellMakers)
		stats.Last5MDistinctBuyMakers = len(cached.BuyMakers)
		stats.Last5MDistinctSellMakers = len(cached.SellMakers)
		stats.Last5MPricePctChange = pricePctChange
	case "6h":
		stats.Last6HTxnCount = cached.BuyCount + cached.SellCount
		stats.Last6HTxnBuyCount = cached.BuyCount
		stats.Last6HTxnSellCount = cached.SellCount
		stats.Last6HTotalVolume = cached.TotalVolume
		stats.Last6HTotalBuyVolume = cached.BuyVolume
		stats.Last6HTotalSellVolume = cached.SellVolume
		stats.Last6HDistinctMakers = len(cached.BuyMakers) + len(cached.SellMakers) - csp.countOverlap(cached.BuyMakers, cached.SellMakers)
		stats.Last6HDistinctBuyMakers = len(cached.BuyMakers)
		stats.Last6HDistinctSellMakers = len(cached.SellMakers)
		stats.Last6HPricePctChange = pricePctChange
	case "24h":
		stats.Last24HTxnCount = cached.BuyCount + cached.SellCount
		stats.Last24HTxnBuyCount = cached.BuyCount
		stats.Last24HTxnSellCount = cached.SellCount
		stats.Last24HTotalVolume = cached.TotalVolume
		stats.Last24HTotalBuyVolume = cached.BuyVolume
		stats.Last24HTotalSellVolume = cached.SellVolume
		stats.Last24HDistinctMakers = len(cached.BuyMakers) + len(cached.SellMakers) - csp.countOverlap(cached.BuyMakers, cached.SellMakers)
		stats.Last24HDistinctBuyMakers = len(cached.BuyMakers)
		stats.Last24HDistinctSellMakers = len(cached.SellMakers)
		stats.Last24HPricePctChange = pricePctChange
	}
}

// applyAggregatedStats applies aggregated statistics to rolling stats
func (csp *CachedStatsProcessor) applyAggregatedStats(timeframe string, aggregated *cache.AggregatedStats, latestTxn models.Transaction, stats *models.RollingStats) {
	csp.applyCachedStats(timeframe, aggregated, latestTxn, stats)
}

// countOverlap counts overlapping elements between two slices
func (csp *CachedStatsProcessor) countOverlap(slice1, slice2 []string) int {
	set := make(map[string]bool)
	for _, s := range slice1 {
		set[s] = true
	}
	count := 0
	for _, s := range slice2 {
		if set[s] {
			count++
		}
	}
	return count
}

// startBackgroundCleanup starts a goroutine to periodically clean up old data
func (csp *CachedStatsProcessor) startBackgroundCleanup() {
	csp.cleanupTicker = time.NewTicker(30 * time.Second) // Cleanup every 30 seconds

	go func() {
		for {
			select {
			case <-csp.cleanupTicker.C:
				csp.cleanupOldData()
			case <-csp.stopCleanup:
				return
			}
		}
	}()
}

// cleanupOldData removes old transactions from buffers and Redis
func (csp *CachedStatsProcessor) cleanupOldData() {
	now := time.Now()
	timeframes := []struct {
		key      string
		duration time.Duration
	}{
		{"1m", 1 * time.Minute},
		{"5m", 5 * time.Minute},
		{"6h", 6 * time.Hour},
		{"24h", 24 * time.Hour},
	}

	csp.bufferMu.Lock()
	defer csp.bufferMu.Unlock()

	for pairID, tfBuffers := range csp.buffers {
		for timeframe := range tfBuffers {
			var duration time.Duration
			for _, tf := range timeframes {
				if tf.key == timeframe {
					duration = tf.duration
					break
				}
			}

			cutoffTime := now.Add(-duration)
			// Buffer will filter automatically on GetRecent, cleanup happens in background

			// Cleanup Redis
			if csp.redisCache != nil {
				go func(pair, tf string, cutoff time.Time) {
					csp.redisCache.RemoveOldTransactions(pair, tf, cutoff)
				}(pairID, timeframe, cutoffTime)
			}
		}
	}
}

// Close stops background cleanup and closes resources
func (csp *CachedStatsProcessor) Close() {
	if csp.cleanupTicker != nil {
		csp.cleanupTicker.Stop()
	}
	close(csp.stopCleanup)
}

