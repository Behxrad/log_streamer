package repository

import (
	"context"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"github.com/Behxrad/log_streamer/configs"
	"github.com/Behxrad/log_streamer/internal/model"
	"time"
)

type MongoLogRepository struct {
	Client *mongo.Client
}

func NewMongoRepository() (*MongoLogRepository, error) {
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

	return &MongoLogRepository{
		Client: client,
	}, nil
}

func (m MongoLogRepository) Close(ctx context.Context) error {
	return m.Client.Disconnect(ctx)
}

func (m MongoLogRepository) InsertLogs(ctx context.Context, logs []model.LogEntry) error {

	_, err := m.Client.Database("log_db").
		Collection("application_logs").
		InsertMany(ctx, logs)

	if err != nil {
		return err
	}
	return nil
}
