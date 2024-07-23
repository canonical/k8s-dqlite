package instrument

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

type Listener struct {
	Network, Address        string
	AcceptedChan            chan net.Conn
	netListener             net.Listener
	bytesWritten, bytesRead atomic.Int64
	wg                      sync.WaitGroup
	err                     error
}

type ListenerMetrics struct {
	BytesWritten int64
	BytesRead    int64
}

func NewListener(network, address string) *Listener {
	return &Listener{
		Network:      network,
		Address:      address,
		AcceptedChan: make(chan net.Conn),
		netListener:  nil,
	}
}

func (l *Listener) Listen() error {
	if l.netListener != nil {
		return fmt.Errorf("listener already running")
	}
	netListener, err := net.Listen(l.Network, l.Address)
	if err != nil {
		return err
	}
	l.netListener = netListener

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for {
			conn, err := l.netListener.Accept()
			if err != nil {
				l.err = err
				break
			}
			l.AcceptedChan <- conn
		}
	}()
	return nil
}

func (l *Listener) Connect(ctx context.Context, _ string) (net.Conn, error) {
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, l.Network, l.Address)
	if err != nil {
		return nil, err
	}
	if conn, ok := conn.(fileConn); ok {
		return &metredConn{conn, l}, nil
	}
	return nil, fmt.Errorf("can't setup probes")
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

func (l *Listener) Close() {
	if l.netListener == nil {
		return
	}

	if err := l.netListener.Close(); err != nil {
		l.err = errors.Join(l.err, err)
	}

	l.wg.Wait()
	l.netListener = nil
	close(l.AcceptedChan)
}

func (l *Listener) Err() error { return l.err }

type fileConn interface {
	net.Conn
	File() (*os.File, error)
}

type metredConn struct {
	fileConn
	listener *Listener
}

func (wc *metredConn) Write(b []byte) (n int, err error) {
	n, err = wc.fileConn.Write(b)
	wc.listener.bytesWritten.Add(int64(n))
	return n, err
}

func (wc *metredConn) Read(b []byte) (n int, err error) {
	n, err = wc.fileConn.Read(b)
	wc.listener.bytesRead.Add(int64(n))
	return n, err
}
