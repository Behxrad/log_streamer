package service

import (
	"github.com/Behxrad/log_streamer/internal/model"
	"sync"
)

type WatchService struct {
	rwMutex sync.RWMutex
	subs    map[string]map[chan *model.LogEntry]struct{}
}

func NewWatchService() *WatchService {
	return &WatchService{
		subs: make(map[string]map[chan *model.LogEntry]struct{}),
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

func (w *WatchService) Subscribe(serviceName string, ch chan *model.LogEntry) {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[serviceName]; !ok {
		w.subs[serviceName] = make(map[chan *model.LogEntry]struct{})
	}
	w.subs[serviceName][ch] = struct{}{}
}

func (w *WatchService) Unsubscribe(serviceName string, ch chan *model.LogEntry) {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[serviceName]; ok {
		delete(w.subs[serviceName], ch)
	}
}
