package service

import (
	"sync"

	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
)

type WatchService struct {
	rwMutex sync.RWMutex
	subs    map[string]map[chan *entity.LogEntry]struct{}
}

func NewWatchService() *WatchService {
	return &WatchService{
		subs: make(map[string]map[chan *entity.LogEntry]struct{}),
	}
}

func (w *WatchService) Publish(logEntry *entity.LogEntry) {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()

	if channels, ok := w.subs[logEntry.ServiceName]; ok {
		for ch := range channels {
			ch <- logEntry
		}
	}
}

func (w *WatchService) Subscribe(serviceName string) chan *entity.LogEntry {
	ch := make(chan *entity.LogEntry, configs.Config.WatcherChannelSize)

	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[serviceName]; !ok {
		w.subs[serviceName] = make(map[chan *entity.LogEntry]struct{})
	}
	w.subs[serviceName][ch] = struct{}{}

	return ch
}

func (w *WatchService) Unsubscribe(serviceName string, ch chan *entity.LogEntry) {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[serviceName]; ok {
		delete(w.subs[serviceName], ch)
	}
	close(ch)
}
