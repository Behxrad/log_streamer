package configs

import "github.com/kelseyhightower/envconfig"

type specification struct {
	GRPCAddress             string `envconfig:"GRPC_ADDRESS" required:"true" default:":50051"`
	GracefulShutDownTimeout int64  `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT" required:"true" default:"120"`
	MongoURI                string `envconfig:"MONGO_URI" required:"true" default:"mongodb://localhost:27017"`
}

var Config specification

func Load() error {
	return envconfig.Process("", &Config)
}
