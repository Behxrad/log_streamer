package log

import (
	"context"
	"log"
	"runtime"
	"sync"

	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
	log_inbound "github.com/Behxrad/log_streamer/internal/port/inbound/log"
	log_outbound "github.com/Behxrad/log_streamer/internal/port/outbound/log"
)

type aggregator struct {
	logRepo   log_outbound.Repository
	ws        log_inbound.Watcher
	logsChan  chan *entity.LogEntry
	closeChan chan bool
}

func NewAggregatorService(logRepo log_outbound.Repository, watchService log_inbound.Watcher) log_inbound.Aggregator {
	aggregator := aggregator{
		logRepo:   logRepo,
		ws:        watchService,
		logsChan:  make(chan *entity.LogEntry, configs.Config.LogAggregatorChannelSize),
		closeChan: make(chan bool),
	}
	go aggregator.initWorkers()
	return aggregator
}

func (l aggregator) Process(ctx context.Context, entry log_inbound.ProcessLogCommand) error {
	logEntry := &entity.LogEntry{
		ServiceName: entry.LogEntry.ServiceName,
		Level:       entry.LogEntry.Level,
		Message:     entry.LogEntry.Message,
		Timestamp:   entry.LogEntry.Timestamp,
		MetaData:    entry.LogEntry.Metadata,
	}
	l.logsChan <- logEntry
	err := l.ws.Publish(ctx, log_inbound.PublishLogCommand{LogEntry: entry.LogEntry})
	if err != nil {
		return err
	}
	return nil
}

func (l aggregator) Close(ctx context.Context) error {
	close(l.logsChan)
	<-l.closeChan
	return nil
}

func (l aggregator) initWorkers() {
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go l.worker(&wg, l.logsChan)
	}
	wg.Wait()
	l.closeChan <- true
}

func (l aggregator) worker(wg *sync.WaitGroup, jobs <-chan *entity.LogEntry) {
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
