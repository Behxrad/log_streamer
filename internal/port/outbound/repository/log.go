package repository

import (
	"context"

	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
	"github.com/Behxrad/log_streamer/pkg/lib"
)

type LogRepository interface {
	lib.Closable
	InsertLogs(context.Context, []entity.LogEntry) error
}
