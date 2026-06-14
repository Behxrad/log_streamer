package app

import (
	"log"
	"github.com/Behxrad/log_streamer/internal/transport/grpc"
)

type App struct {
	gRPCServer grpc.Server
}

func NewApp() *App {
	return &App{
		gRPCServer: grpc.NewRPCServer(),
	}
}

func (a *App) Start() {
	log.Println("starting application")
	go func() {
		err := a.gRPCServer.Serve()
		if err != nil {
			log.Fatalf("%s\n%v", "grpc server did not start successfully", err)
		}
	}()
	log.Println("grpc server started successfully")

	log.Println("application started")
}

func (a *App) Stop() {
	log.Println("Stopping application")
	err := a.gRPCServer.Shutdown()
	if err != nil {
		return
	}
	log.Println("application stopped")
}
