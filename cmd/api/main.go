package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/app"
)

func main() {
	err := configs.Load()
	if err != nil {
		log.Fatalf("%s\n%v", "config did not load successfully", err)
	}
	bgCTX := context.Background()

	application := app.NewApp()
	err = application.Start(bgCTX)
	if err != nil {
		log.Fatalf("%s\n%v", "failed to start the application", err)
	}

	ListenForGracefulShutdown(application)
}

func ListenForGracefulShutdown(application app.App) {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	done := make(chan bool)

	go func() {
		err := application.Stop(context.Background())
		if err != nil {
			log.Println(err)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second * time.Duration(configs.Config.GracefulShutDownTimeout)):
	}
}
