// Package vp8channel provides reliable byte transport over VP8 video frames using KCP.
package vp8channel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/spkprsnts/vk-turn-proxy/internal/logger"
	kcp "github.com/xtaci/kcp-go/v5"
)

// Both peers use the same conv ID. KCP does not require a handshake —
// packets are matched by conv field, giving us a symmetrical P2P setup.
const kcpConvID = 0xC0FFEE01

// KCP tuning targets a lossy, bursty carrier (VP8 over an SFU).
const (
	// pion VP8 packetizer splits samples larger than ~1200 bytes into multiple RTP
	// packets. If any packet in a multi-RTP frame is lost, processRTPPacket discards
	// the entire frame (sequence gap check), forcing KCP to retransmit all of it.
	// Keeping MTU+epochHdrLen ≤ 1200 ensures 1 RTP packet per KCP segment, so a
	// single loss only costs one segment retransmit, not a whole burst.
	// 1100 + 29 (epochHdr) = 1129 bytes < 1200 ← safe margin.
	kcpMTU = 1100

	// BDP at 20 Mbps / 100 ms RTT ≈ 232 segments @ MTU 1100 (MSS 1076 bytes).
	// 1024 gives 4× headroom for RTT spikes; max in-flight ~1.1 MB → drains in
	// ~0.45 s at 20 Mbps. Covers RTT fluctuations up to ~440 ms.
	kcpSndWnd = 1024
	kcpRcvWnd = 1024

	// Length prefix for message framing on top of the KCP stream.
	// We use stream mode because UDPSession.Write fragments messages > MSS
	// outside of kcp.Send, which destroys the frg field that message mode
	// relies on for boundary preservation.
	kcpLenPrefix = 4

	// Hard cap on a single message — anything larger is almost certainly
	// a protocol error upstream.
	kcpMaxMessage = 8 * 1024 * 1024
)

var ErrKCPMessageTooLarge = errors.New("vp8channel: kcp message exceeds maximum size")

// kcpRuntime owns the KCP session and the goroutine that reassembles
// messages and delivers them to cfg.OnData.
type kcpRuntime struct {
	conn      *kcpConn
	sess      *kcp.UDPSession
	readDone  chan struct{}
	writeMu   sync.Mutex // serialises length-prefix + payload writes
	closeOnce sync.Once
}

func startKCP(out chan<- []byte, onData func([]byte), epochHdr [epochHdrLen]byte) (*kcpRuntime, error) {
	c := newKCPConn(out, inboundQueueSize, epochHdr)

	sess, err := kcp.NewConn3(kcpConvID, fakeUDPAddr(), nil, 0, 0, c)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("kcp new conn: %w", err)
	}

	// Aggressive ARQ: nodelay=1, interval=5 ms, fast-resend=2, no CC.
	// 5 ms tick halves worst-case scheduling latency vs the 10 ms default,
	// which matters for SOCKS5 proxying (~3 RTTs before first response byte).
	sess.SetNoDelay(1, 5, 2, 1)
	sess.SetWindowSize(kcpSndWnd, kcpRcvWnd)
	sess.SetMtu(kcpMTU)
	sess.SetACKNoDelay(true)
	sess.SetWriteDelay(false)

	rt := &kcpRuntime{
		conn:     c,
		sess:     sess,
		readDone: make(chan struct{}),
	}
	go rt.readLoop(onData)
	return rt, nil
}

func (r *kcpRuntime) readLoop(onData func([]byte)) {
	defer close(r.readDone)

	var hdr [kcpLenPrefix]byte
	for {
		if _, err := io.ReadFull(r.sess, hdr[:]); err != nil {
			logger.Debugf("KCP readLoop ended: %v", err)
			return
		}
		size := binary.BigEndian.Uint32(hdr[:])
		if size == 0 {
			continue
		}
		if size > kcpMaxMessage {
			logger.Debugf("KCP readLoop: oversized message %d, bailing", size)
			return
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r.sess, payload); err != nil {
			logger.Debugf("KCP readLoop payload read ended: %v", err)
			return
		}
		logger.Debugf("KCP readLoop delivered message len=%d", size)
		if onData != nil {
			onData(payload)
		}
	}
}

// deliver hands a wire payload (already reassembled from VP8 RTP) to KCP.
func (r *kcpRuntime) deliver(payload []byte) {
	r.conn.deliver(payload)
}

// send queues an application message for reliable delivery.
// Header and payload are merged into one slice so a single sess.Write() call
// is made — with SetWriteDelay(false) each Write triggers a KCP flush, so two
// separate writes would produce two VP8 frames where one suffices.
func (r *kcpRuntime) send(msg []byte) error {
	if len(msg) > kcpMaxMessage {
		return ErrKCPMessageTooLarge
	}
	frame := make([]byte, kcpLenPrefix+len(msg))
	//nolint:gosec
	binary.BigEndian.PutUint32(frame[:kcpLenPrefix], uint32(len(msg)))
	copy(frame[kcpLenPrefix:], msg)

	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	if _, err := r.sess.Write(frame); err != nil {
		return fmt.Errorf("kcp write: %w", err)
	}
	return nil
}

func (r *kcpRuntime) close() {
	r.closeOnce.Do(func() {
		_ = r.sess.Close()
		_ = r.conn.Close()
	})
}
