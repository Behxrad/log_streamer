package log

import (
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
	Publish(cmd PublishLogCommand) error
	Subscribe(query SubscribeLogsQuery) (chan *LogEntry, error)
	Unsubscribe(cmd UnsubscribeLogsCommand) error
}
