package bus

import (
	"context"
	"log"

	eventschema "agentmesh/shared/event-schema"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, event eventschema.Event) error
}

type LogPublisher struct {
	logger *log.Logger
}

func NewLogPublisher(logger *log.Logger) *LogPublisher {
	return &LogPublisher{logger: logger}
}

func (p *LogPublisher) Publish(ctx context.Context, topic string, event eventschema.Event) error {
	_ = ctx
	p.logger.Printf("published topic=%s event_id=%s correlation_id=%v", topic, event.ID, event.Payload["correlation_id"])
	return nil
}
