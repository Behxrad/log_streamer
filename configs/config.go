package configs

import "github.com/kelseyhightower/envconfig"

type Specification struct {
	GRPCAddress             string `envconfig:"GRPC_ADDRESS" required:"true" default:"50051"`
	GracefulShutDownTimeout int64  `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT" required:"true" default:"120"`
}

var Config Specification

func Load() error {
	return envconfig.Process("", &Config)
}
