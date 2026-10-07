package log

import (
	"context"
	"time"
)

type PublishLogCommand struct {
	LogEntry *LogEntry
}

type LogEntry struct {
	ServiceName string
	Level       string
	Message     string
	Timestamp   time.Time
	Metadata    map[string]string
}

type SubscribeLogsQuery struct {
	ServiceName string
}

type UnsubscribeLogsCommand struct {
	ServiceName string
	Stream      chan *LogEntry
}

type Watcher interface {
	Publish(context.Context, PublishLogCommand) error
	Subscribe(context.Context, SubscribeLogsQuery) (chan *LogEntry, error)
	Unsubscribe(context.Context, UnsubscribeLogsCommand) error
}
