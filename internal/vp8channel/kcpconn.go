package vp8channel

import (
	"net"
	"sync"
	"time"
)

func fakeUDPAddr() *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

// kcpConn is a net.PacketConn that bridges kcp-go on top of the VP8 channel.
//
//	kcp.UDPSession  ──Write──▶  WriteTo  ──▶ outbound chan  ──▶ VP8 wire
//	kcp.UDPSession  ◀──Read──   ReadFrom  ◀── deliver()     ◀── VP8 wire
//
// Packet boundaries are preserved by the channel, which is exactly what
// KCP expects from a UDP-like transport.
type kcpConn struct {
	out       chan<- []byte
	in        chan []byte
	closed    chan struct{}
	closeOnce sync.Once

	// epochHdr is prepended to every outgoing KCP packet so the peer can
	// detect a session restart on our side (see transport.go for layout).
	epochHdr [epochHdrLen]byte

	mu        sync.Mutex
	rDeadline time.Time
	wDeadline time.Time
}

func newKCPConn(out chan<- []byte, inboundCap int, epochHdr [epochHdrLen]byte) *kcpConn {
	if inboundCap <= 0 {
		inboundCap = 1024
	}
	return &kcpConn{
		out:      out,
		in:       make(chan []byte, inboundCap),
		closed:   make(chan struct{}),
		epochHdr: epochHdr,
	}
}

// deliver hands an incoming wire payload to the KCP read loop.
// Drops on overflow are intentional — KCP will detect the loss via SACK and retransmit.
func (c *kcpConn) deliver(payload []byte) {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	select {
	case c.in <- cp:
	case <-c.closed:
	default:
	}
}

func (c *kcpConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.mu.Lock()
	deadline := c.rDeadline
	c.mu.Unlock()

	var timerC <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, nil, TimeoutError{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timerC = t.C
	}

	select {
	case msg := <-c.in:
		n := copy(p, msg)
		return n, fakeUDPAddr(), nil
	case <-c.closed:
		return 0, nil, net.ErrClosed
	case <-timerC:
		return 0, nil, TimeoutError{}
	}
}

func (c *kcpConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	buf := make([]byte, epochHdrLen+len(p))
	copy(buf, c.epochHdr[:])
	copy(buf[epochHdrLen:], p)

	c.mu.Lock()
	deadline := c.wDeadline
	c.mu.Unlock()

	var timerC <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return 0, TimeoutError{}
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timerC = t.C
	}

	select {
	case c.out <- buf:
		return len(p), nil
	case <-c.closed:
		return 0, net.ErrClosed
	case <-timerC:
		return 0, TimeoutError{}
	}
}

func (c *kcpConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *kcpConn) LocalAddr() net.Addr { return fakeUDPAddr() }

func (c *kcpConn) SetDeadline(t time.Time) error {
	_ = c.SetReadDeadline(t)
	_ = c.SetWriteDeadline(t)
	return nil
}

func (c *kcpConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.rDeadline = t
	c.mu.Unlock()
	return nil
}

func (c *kcpConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.wDeadline = t
	c.mu.Unlock()
	return nil
}

// TimeoutError is a net.Error indicating a deadline exceeded.
type TimeoutError struct{}

func (TimeoutError) Error() string   { return "i/o timeout" }
func (TimeoutError) Timeout() bool   { return true }
func (TimeoutError) Temporary() bool { return true }
