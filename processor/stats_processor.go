package processor

import (
	"solana-txn-interra/models"
	"time"

	"github.com/google/uuid"
)

// TimeframeStats holds statistics for a specific timeframe
type TimeframeStats struct {
	Transactions     []models.Transaction
	BuyTransactions  []models.Transaction
	SellTransactions []models.Transaction
	BuyMakers        map[string]bool
	SellMakers       map[string]bool
	OpenPrice        float64
	OpenPriceSet     bool
}

// StatsProcessor processes transactions and calculates rolling statistics
type StatsProcessor struct {
	// In-memory storage: pair_id -> timeframe -> stats
	storage map[string]map[string]*TimeframeStats
}

// NewStatsProcessor creates a new stats processor
func NewStatsProcessor() *StatsProcessor {
	return &StatsProcessor{
		storage: make(map[string]map[string]*TimeframeStats),
	}
}

// ProcessTransaction processes a transaction and updates rolling statistics
func (sp *StatsProcessor) ProcessTransaction(txn models.Transaction) *models.RollingStats {
	now := time.Now()
	pairID := txn.PairID

	// Initialize storage for pair if needed
	if sp.storage[pairID] == nil {
		sp.storage[pairID] = make(map[string]*TimeframeStats)
	}

	// Define timeframes
	timeframes := []struct {
		key      string
		startKey string
		duration time.Duration
	}{
		{"1m", "Start1M", 1 * time.Minute},
		{"5m", "Start5M", 5 * time.Minute},
		{"6h", "Start6H", 6 * time.Hour},
		{"24h", "Start24H", 24 * time.Hour},
	}

	// Update stats for each timeframe
	for _, tf := range timeframes {
		sp.updateTimeframeStats(pairID, tf.key, txn, now, tf.duration)
	}

	// Calculate and return rolling stats
	return sp.calculateRollingStats(pairID, txn, now)
}

// updateTimeframeStats updates statistics for a specific timeframe
func (sp *StatsProcessor) updateTimeframeStats(pairID, timeframe string, txn models.Transaction, now time.Time, duration time.Duration) {
	stats := sp.storage[pairID][timeframe]
	if stats == nil {
		stats = &TimeframeStats{
			Transactions:     []models.Transaction{},
			BuyTransactions:  []models.Transaction{},
			SellTransactions: []models.Transaction{},
			BuyMakers:        make(map[string]bool),
			SellMakers:       make(map[string]bool),
		}
		sp.storage[pairID][timeframe] = stats
	}

	// Calculate cutoff time for rolling window
	cutoffTime := now.Add(-duration)

	// Remove old transactions outside the timeframe window
	stats.Transactions = sp.filterByTime(stats.Transactions, cutoffTime)
	stats.BuyTransactions = sp.filterByTime(stats.BuyTransactions, cutoffTime)
	stats.SellTransactions = sp.filterByTime(stats.SellTransactions, cutoffTime)

	// Rebuild maker sets after filtering
	stats.BuyMakers = make(map[string]bool)
	stats.SellMakers = make(map[string]bool)
	for _, t := range stats.BuyTransactions {
		stats.BuyMakers[t.Maker] = true
	}
	for _, t := range stats.SellTransactions {
		stats.SellMakers[t.Maker] = true
	}

	// Add new transaction if it's within the timeframe window
	if txn.Timestamp.After(cutoffTime) || txn.Timestamp.Equal(cutoffTime) {
		stats.Transactions = append(stats.Transactions, txn)

		if txn.Type == "buy" {
			stats.BuyTransactions = append(stats.BuyTransactions, txn)
			stats.BuyMakers[txn.Maker] = true
		} else if txn.Type == "sell" {
			stats.SellTransactions = append(stats.SellTransactions, txn)
			stats.SellMakers[txn.Maker] = true
		}

		// Update open price: find the oldest transaction in the current window
		if len(stats.Transactions) > 0 {
			oldestTxn := stats.Transactions[0]
			for _, t := range stats.Transactions {
				if t.Timestamp.Before(oldestTxn.Timestamp) {
					oldestTxn = t
				}
			}
			stats.OpenPrice = oldestTxn.PriceUSD
			stats.OpenPriceSet = true
		}
	}
}

// filterByTime filters transactions that are after the cutoff time
func (sp *StatsProcessor) filterByTime(transactions []models.Transaction, cutoffTime time.Time) []models.Transaction {
	filtered := []models.Transaction{}
	for _, txn := range transactions {
		if txn.Timestamp.After(cutoffTime) || txn.Timestamp.Equal(cutoffTime) {
			filtered = append(filtered, txn)
		}
	}
	return filtered
}

// calculateRollingStats calculates rolling statistics for all timeframes
func (sp *StatsProcessor) calculateRollingStats(pairID string, latestTxn models.Transaction, now time.Time) *models.RollingStats {
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
	timeframes := []string{"1m", "5m", "6h", "24h"}
	for _, tf := range timeframes {
		tfStats := sp.storage[pairID][tf]
		if tfStats == nil {
			continue
		}

		// Calculate totals
		var totalVolume, buyVolume, sellVolume float64
		for _, txn := range tfStats.Transactions {
			totalVolume += txn.AmountUSD
			if txn.Type == "buy" {
				buyVolume += txn.AmountUSD
			} else if txn.Type == "sell" {
				sellVolume += txn.AmountUSD
			}
		}

		// Calculate distinct makers
		allMakers := make(map[string]bool)
		for maker := range tfStats.BuyMakers {
			allMakers[maker] = true
		}
		for maker := range tfStats.SellMakers {
			allMakers[maker] = true
		}

		// Calculate price change percentage
		var pricePctChange float64
		if tfStats.OpenPriceSet && tfStats.OpenPrice > 0 {
			pricePctChange = ((latestTxn.PriceUSD - tfStats.OpenPrice) / tfStats.OpenPrice) * 100
		}

		// Set stats based on timeframe
		switch tf {
		case "1m":
			stats.Last1MTxnCount = len(tfStats.Transactions)
			stats.Last1MTxnBuyCount = len(tfStats.BuyTransactions)
			stats.Last1MTxnSellCount = len(tfStats.SellTransactions)
			stats.Last1MTotalVolume = totalVolume
			stats.Last1MTotalBuyVolume = buyVolume
			stats.Last1MTotalSellVolume = sellVolume
			stats.Last1MDistinctMakers = len(allMakers)
			stats.Last1MDistinctBuyMakers = len(tfStats.BuyMakers)
			stats.Last1MDistinctSellMakers = len(tfStats.SellMakers)
			stats.Last1MPricePctChange = pricePctChange
		case "5m":
			stats.Last5MTxnCount = len(tfStats.Transactions)
			stats.Last5MTxnBuyCount = len(tfStats.BuyTransactions)
			stats.Last5MTxnSellCount = len(tfStats.SellTransactions)
			stats.Last5MTotalVolume = totalVolume
			stats.Last5MTotalBuyVolume = buyVolume
			stats.Last5MTotalSellVolume = sellVolume
			stats.Last5MDistinctMakers = len(allMakers)
			stats.Last5MDistinctBuyMakers = len(tfStats.BuyMakers)
			stats.Last5MDistinctSellMakers = len(tfStats.SellMakers)
			stats.Last5MPricePctChange = pricePctChange
		case "6h":
			stats.Last6HTxnCount = len(tfStats.Transactions)
			stats.Last6HTxnBuyCount = len(tfStats.BuyTransactions)
			stats.Last6HTxnSellCount = len(tfStats.SellTransactions)
			stats.Last6HTotalVolume = totalVolume
			stats.Last6HTotalBuyVolume = buyVolume
			stats.Last6HTotalSellVolume = sellVolume
			stats.Last6HDistinctMakers = len(allMakers)
			stats.Last6HDistinctBuyMakers = len(tfStats.BuyMakers)
			stats.Last6HDistinctSellMakers = len(tfStats.SellMakers)
			stats.Last6HPricePctChange = pricePctChange
		case "24h":
			stats.Last24HTxnCount = len(tfStats.Transactions)
			stats.Last24HTxnBuyCount = len(tfStats.BuyTransactions)
			stats.Last24HTxnSellCount = len(tfStats.SellTransactions)
			stats.Last24HTotalVolume = totalVolume
			stats.Last24HTotalBuyVolume = buyVolume
			stats.Last24HTotalSellVolume = sellVolume
			stats.Last24HDistinctMakers = len(allMakers)
			stats.Last24HDistinctBuyMakers = len(tfStats.BuyMakers)
			stats.Last24HDistinctSellMakers = len(tfStats.SellMakers)
			stats.Last24HPricePctChange = pricePctChange
		}
	}

	return stats
}
