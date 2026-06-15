package configs

import "github.com/kelseyhightower/envconfig"

type specification struct {
	GRPCAddress             string `envconfig:"GRPC_ADDRESS" required:"true" default:":50051"`
	GracefulShutDownTimeout int    `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT" required:"true" default:"120"`
	MongoURI                string `envconfig:"MONGO_URI" required:"true" default:"mongodb://localhost:27017"`

	LogAggregatorChannelSize int `envconfig:"LOG_AGGREGATOR_CHANNEL_SIZE" required:"true" default:"1000"`
	LogAggregatorBatchSize   int `envconfig:"LOG_AGGREGATOR_BATCH_SIZE" required:"true" default:"100"`

	WatcherChannelSize int `envconfig:"WATCHER_CHANNEL_SIZE" required:"true" default:"1000"`
}

var Config specification

func Load() error {
	return envconfig.Process("", &Config)
}
