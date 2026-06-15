package service

import (
	"context"
	"log"
	"github.com/Behxrad/log_streamer/internal/model"
	"github.com/Behxrad/log_streamer/internal/repository"
	"runtime"
	"sync"
)

type LogAggregator struct {
	logRepo   repository.LogRepository
	ws        *WatchService
	logsChan  chan *model.LogEntry
	closeChan chan bool
}

func NewLogAggregator(logRepo repository.LogRepository, watchService *WatchService) LogAggregator {
	aggregator := LogAggregator{
		logRepo:   logRepo,
		ws:        watchService,
		logsChan:  make(chan *model.LogEntry, 100), //TODO: make the size configurable
		closeChan: make(chan bool),
	}
	go aggregator.initWorkers()
	return aggregator
}

func (l LogAggregator) Process(entry *model.LogEntry) {
	l.logsChan <- entry
	l.ws.Publish(entry)
}

func (l LogAggregator) Close(ctx context.Context) error {
	close(l.logsChan)
	<-l.closeChan
	return nil
}

func (l LogAggregator) initWorkers() {
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go l.worker(&wg, l.logsChan)
	}
	wg.Wait()
	l.closeChan <- true
}

func (l LogAggregator) worker(wg *sync.WaitGroup, jobs <-chan *model.LogEntry) {
	defer wg.Done()
	batch := make([]model.LogEntry, 0, 5) //TODO: make the size configurable

	for entry := range jobs {
		batch = append(batch, *entry)

		if len(batch) == cap(batch) {
			err := l.logRepo.InsertLogs(context.Background(), batch)
			if err != nil {
				log.Println(err) //TODO: log errors
			}
			batch = batch[:0]
		}
	}
}
