package service

import (
	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/model"
	"sync"
)

type WatchService struct {
	rwMutex sync.RWMutex
	subs    map[string]map[chan *model.LogEntry]interface{}
}

func NewWatchService() *WatchService {
	return &WatchService{
		subs: make(map[string]map[chan *model.LogEntry]interface{}),
	}
}

func (w *WatchService) Publish(logEntry *model.LogEntry) {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()

	if channels, ok := w.subs[logEntry.ServiceName]; ok {
		for ch := range channels {
			ch <- logEntry
		}
	}
}

func (w *WatchService) Subscribe(serviceName string) chan *model.LogEntry {
	ch := make(chan *model.LogEntry, configs.Config.WatcherChannelSize)

	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[serviceName]; !ok {
		w.subs[serviceName] = make(map[chan *model.LogEntry]interface{})
	}
	w.subs[serviceName][ch] = struct{}{}

	return ch
}

func (w *WatchService) Unsubscribe(serviceName string, ch chan *model.LogEntry) {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[serviceName]; ok {
		delete(w.subs[serviceName], ch)
	}
	close(ch)
}
