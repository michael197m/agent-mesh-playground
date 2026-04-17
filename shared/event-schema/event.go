package eventschema

import "time"

type EventStatus string

const (
	EventStatusPending   EventStatus = "pending"
	EventStatusRunning   EventStatus = "running"
	EventStatusSucceeded EventStatus = "succeeded"
	EventStatusFailed    EventStatus = "failed"
)

type Event struct {
	ID        string         `json:"id"`
	Topic     string         `json:"topic"`
	Status    EventStatus    `json:"status"`
	Source    string         `json:"source"`
	Timestamp time.Time      `json:"timestamp"`
	Payload   map[string]any `json:"payload,omitempty"`
}
