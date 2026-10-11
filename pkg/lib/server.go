package lib

type Server interface {
	Serve() error
	Closable
}
