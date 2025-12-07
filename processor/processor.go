package processor

import "solana-txn-interra/models"

// Processor defines the interface for transaction processors
type Processor interface {
	ProcessTransaction(txn models.Transaction) *models.RollingStats
}

