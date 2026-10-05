package log

import (
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
)

type Aggregator interface {
	Process(entry *entity.LogEntry)
}
