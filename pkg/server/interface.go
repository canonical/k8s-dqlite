package server

import "context"

type Instance interface {
	Start(context.Context) error
	MustStop() <-chan struct{}
	Shutdown(context.Context) error
}
