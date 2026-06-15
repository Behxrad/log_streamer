package lib

import "context"

type Closable interface {
	Close(context.Context) error
}
