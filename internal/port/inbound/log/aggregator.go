package log

import (
	"context"

	"github.com/Behxrad/log_streamer/pkg/lib"
)

type ProcessLogCommand struct {
	LogEntry *LogEntry
}

type Aggregator interface {
	lib.Closable
	Process(context.Context, ProcessLogCommand) error
}
