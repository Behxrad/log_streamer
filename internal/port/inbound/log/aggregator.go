package log

import (
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
	"github.com/Behxrad/log_streamer/pkg/lib"
)

type Aggregator interface {
	lib.Closable
	Process(entry *entity.LogEntry)
}
