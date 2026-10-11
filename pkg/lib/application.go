package lib

import "context"

type Application interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}
