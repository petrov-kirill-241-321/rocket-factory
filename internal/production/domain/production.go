package domain

import "time"

const (
	StatusStarted   = "started"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

type Task struct {
	ID          string
	OrderID     string
	UserID      string
	Status      string
	StartedAt   *time.Time
	CompletedAt *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
