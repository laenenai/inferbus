package harvester

import "time"

// Config controls the harvester's batching behavior. Task 6 extends this
// file with yaml tags and a LoadConfig function; for now it holds just the
// fields the batcher (Task 4) and budget ledger (Task 5) need.
type Config struct {
	// BatchMaxEvents is the number of accumulated events that triggers an
	// immediate flush to the sink, regardless of BatchMaxInterval.
	BatchMaxEvents int

	// BatchMaxInterval is the maximum time accumulated events sit
	// unflushed before a flush is forced, regardless of BatchMaxEvents.
	BatchMaxInterval time.Duration

	// BudgetRefreshInterval controls how often the budget ledger (Task 5)
	// reloads budgets from the KV store. Unused by the batcher itself but
	// defined here so Config stays in one place.
	BudgetRefreshInterval time.Duration
}

// Default values applied by withDefaults for any zero-valued Config field.
const (
	defaultBatchMaxEvents        = 500
	defaultBatchMaxInterval      = 2 * time.Second
	defaultBudgetRefreshInterval = 10 * time.Second
)

// withDefaults returns a copy of c with any unset (zero-valued) field
// replaced by its default.
func (c Config) withDefaults() Config {
	if c.BatchMaxEvents <= 0 {
		c.BatchMaxEvents = defaultBatchMaxEvents
	}
	if c.BatchMaxInterval <= 0 {
		c.BatchMaxInterval = defaultBatchMaxInterval
	}
	if c.BudgetRefreshInterval <= 0 {
		c.BudgetRefreshInterval = defaultBudgetRefreshInterval
	}
	return c
}
