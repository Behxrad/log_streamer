package log

import (
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
)

type Watcher interface {
	Publish(logEntry *entity.LogEntry)
	Subscribe(serviceName string) chan *entity.LogEntry
	Unsubscribe(serviceName string, ch chan *entity.LogEntry)
}
