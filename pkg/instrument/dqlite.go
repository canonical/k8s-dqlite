package instrument

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

type Listener struct {
	Network, Address        string
	AcceptChan              chan net.Conn
	listener                net.Listener
	bytesWritten, bytesRead atomic.Int64
	wg                      sync.WaitGroup
}

type ListenerMetrics struct {
	BytesWritten int64
	BytesRead    int64
}

func Listen(network, address string) (*Listener, error) {
	netListener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	listener := &Listener{
		Network:    network,
		Address:    address,
		AcceptChan: make(chan net.Conn),
		listener:   netListener,
	}

	listener.wg.Add(1)
	go func() {
		listener.run()
		listener.wg.Done()
	}()

	return listener, nil
}

func (l *Listener) Connect(ctx context.Context, _ string) (net.Conn, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, l.Network, l.Address)
	if err != nil {
		return nil, err
	}
	if conn, ok := conn.(fileConn); ok {
		return &wrappedConn{conn, l}, nil
	}
	return nil, fmt.Errorf("can't setup probes")
}

func (l *Listener) run() {
	defer close(l.AcceptChan)
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			break
		}
		l.AcceptChan <- conn
	}
}

func (l *Listener) Metrics() *ListenerMetrics {
	return &ListenerMetrics{
		BytesWritten: l.bytesWritten.Load(),
		BytesRead:    l.bytesRead.Load(),
	}
}

func (l *Listener) ResetMetrics() {
	l.bytesWritten.Store(0)
	l.bytesRead.Store(0)
}

func (l *Listener) Close() error {
	if err := l.listener.Close(); err != nil {
		return err
	}

	l.wg.Wait()
	return nil
}

type fileConn interface {
	net.Conn
	File() (*os.File, error)
}

type wrappedConn struct {
	fileConn
	listener *Listener
}

func (wc *wrappedConn) Write(b []byte) (n int, err error) {
	n, err = wc.fileConn.Write(b)
	wc.listener.bytesWritten.Add(int64(n))
	return n, err
}

func (wc *wrappedConn) Read(b []byte) (n int, err error) {
	n, err = wc.fileConn.Read(b)
	wc.listener.bytesRead.Add(int64(n))
	return n, err
}
