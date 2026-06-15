package repository

import (
	"context"
	"github.com/Behxrad/log_streamer/internal/model"
	"github.com/Behxrad/log_streamer/pkg/lib"
)

type LogRepository interface {
	lib.Closable
	InsertLogs(context.Context, []model.LogEntry) error
}
