package log

import (
	"context"
	"sync"

	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/port/inbound/log"
)

type watcher struct {
	rwMutex sync.RWMutex
	subs    map[string]map[chan *log.LogEntry]struct{}
}

func NewWatcherService() log.Watcher {
	return &watcher{
		subs: make(map[string]map[chan *log.LogEntry]struct{}),
	}
}

func (w *watcher) Publish(ctx context.Context, cmd log.PublishLogCommand) error {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()

	if channels, ok := w.subs[cmd.LogEntry.ServiceName]; ok {
		for ch := range channels {
			ch <- cmd.LogEntry
		}
	}
	return nil
}

func (w *watcher) Subscribe(ctx context.Context, query log.SubscribeLogsQuery) (chan *log.LogEntry, error) {
	ch := make(chan *log.LogEntry, configs.Config.WatcherChannelSize)

	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[query.ServiceName]; !ok {
		w.subs[query.ServiceName] = make(map[chan *log.LogEntry]struct{})
	}
	w.subs[query.ServiceName][ch] = struct{}{}

	return ch, nil
}

func (w *watcher) Unsubscribe(ctx context.Context, cmd log.UnsubscribeLogsCommand) error {
	w.rwMutex.Lock()
	defer w.rwMutex.Unlock()
	if _, ok := w.subs[cmd.ServiceName]; ok {
		delete(w.subs[cmd.ServiceName], cmd.Stream)
	}
	close(cmd.Stream)
	return nil
}
