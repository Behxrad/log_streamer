package log

import (
	"github.com/Behxrad/log_streamer/pkg/lib"
)

type ProcessLogCommand struct {
	LogEntry *LogEntry
}

type Aggregator interface {
	lib.Closable
	Process(cmd ProcessLogCommand) error
}
