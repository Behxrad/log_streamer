package app

import (
	"context"
	"log"
	"github.com/Behxrad/log_streamer/internal/repository"
	"github.com/Behxrad/log_streamer/internal/service"
	"github.com/Behxrad/log_streamer/internal/transport/grpc"
)

type App struct {
	gRPCServer grpc.Server
	stop       func(context.Context) error
}

func NewApp() App {
	mongoRepository, err := repository.NewMongoRepository()
	if err != nil {
		log.Fatal(err)
		return App{}
	}
	aggregator := service.NewLogAggregator(mongoRepository)
	gRPCServer := grpc.NewRPCServer(aggregator)

	return App{
		gRPCServer: gRPCServer,
		stop: func(ctx context.Context) error {
			err := gRPCServer.Close(ctx)
			if err != nil {
				return err
			}
			err = aggregator.Close(ctx)
			if err != nil {
				return err
			}
			err = mongoRepository.Close(ctx)
			if err != nil {
				return err
			}
			return nil
		},
	}
}

func (a App) Start(ctx context.Context) error {
	log.Println("starting application")
	go func() {
		err := a.gRPCServer.Serve()
		if err != nil {
			log.Fatalf("%s\n\t%v", "grpc server did not start successfully", err)
		}
	}()
	log.Println("grpc server started successfully")

	log.Println("application started")
	return nil
}

func (a App) Stop(ctx context.Context) error {
	log.Println("stopping application")
	err := a.stop(ctx)
	if err != nil {
		return err
	}
	log.Println("application stopped")
	return nil
}
