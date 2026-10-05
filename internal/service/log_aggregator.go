package service

import (
	"context"
	"log"
	"runtime"
	"sync"

	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
	"github.com/Behxrad/log_streamer/internal/port/outbound/repository"
)

type LogAggregator struct {
	logRepo   repository.LogRepository
	ws        *WatchService
	logsChan  chan *entity.LogEntry
	closeChan chan bool
}

func NewLogAggregator(logRepo repository.LogRepository, watchService *WatchService) LogAggregator {
	aggregator := LogAggregator{
		logRepo:   logRepo,
		ws:        watchService,
		logsChan:  make(chan *entity.LogEntry, configs.Config.LogAggregatorChannelSize),
		closeChan: make(chan bool),
	}
	go aggregator.initWorkers()
	return aggregator
}

func (l LogAggregator) Process(entry *entity.LogEntry) {
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

func (l LogAggregator) worker(wg *sync.WaitGroup, jobs <-chan *entity.LogEntry) {
	defer wg.Done()
	batch := make([]entity.LogEntry, 0, configs.Config.LogAggregatorBatchSize)

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
