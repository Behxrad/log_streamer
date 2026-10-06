package repository

import (
	"context"
	"time"

	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/domain/model/entity"
	log_outbound "github.com/Behxrad/log_streamer/internal/port/outbound/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoLogRepository struct {
	Client *mongo.Client
}

func NewMongoLogRepository() (log_outbound.Repository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(configs.Config.MongoURI).
		SetMaxPoolSize(100).
		SetMinPoolSize(10).
		SetMaxConnIdleTime(30 * time.Minute)

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	return &mongoLogRepository{
		Client: client,
	}, nil
}

func (m mongoLogRepository) Close(ctx context.Context) error {
	return m.Client.Disconnect(ctx)
}

func (m mongoLogRepository) InsertLogs(ctx context.Context, logs []entity.LogEntry) error {

	_, err := m.Client.Database("log_db").
		Collection("application_logs").
		InsertMany(ctx, logs)

	if err != nil {
		return err
	}
	return nil
}
