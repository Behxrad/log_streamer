package entity

import "time"

type LogEntry struct {
	ServiceName string            `bson:"service_name"`
	Level       string            `bson:"level"`
	Message     string            `bson:"message"`
	Timestamp   time.Time         `bson:"timestamp"`
	MetaData    map[string]string `json:"metadata"`
}
