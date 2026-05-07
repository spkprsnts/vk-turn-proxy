// Package vp8channel provides a best-effort byte transport over a VP8 video track.
// Data is prefixed with a marker byte and length field inside raw VP8 frames;
// keepalive frames carry a minimal valid VP8 key frame so the SFU stays happy.
package vp8channel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const (
	maxPayloadSize   = 60 * 1024
	connectTimeout   = 30 * time.Second
	dataMarker       = byte(0xFF)
	rtpBufSize       = 65536
	tickInterval     = 10 * time.Millisecond // 100 fps; was 40ms (25 fps)
	sampleDuration   = 10 * time.Millisecond
	maxFramesPerTick = 16
)

var (
	ErrTransportClosed = errors.New("vp8channel transport closed")
	ErrOutboundFull    = errors.New("vp8channel outbound full")

	// vp8Keepalive is a minimal valid 16×16 VP8 key frame used as idle heartbeat.
	vp8Keepalive = []byte{
		0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00,
		0x10, 0x00, 0x00, 0x47, 0x08, 0x85, 0x85, 0x88,
		0x99, 0x84, 0x88, 0xfc,
	}
)

// Session is the underlying WebRTC provider the VP8 transport runs on top of.
// *telemost.Peer satisfies this interface.
type Session interface {
	Connect(ctx context.Context) error
	Close() error
	SetReconnectCallback(cb func(*webrtc.DataChannel))
	SetShouldReconnect(fn func() bool)
	SetEndedCallback(cb func(string))
	WatchConnection(ctx context.Context)
	CanSend() bool
	AddVideoTrack(track webrtc.TrackLocal) error
	SetVideoTrackHandler(cb func(*webrtc.TrackRemote, *webrtc.RTPReceiver))
}

// Transport carries arbitrary byte messages over a WebRTC VP8 video track.
// Delivery is best-effort (no ACK, no retransmission).
type Transport struct {
	session    Session
	track      *webrtc.TrackLocalStaticSample
	onData     func([]byte)
	outbound   chan []byte
	closeCh    chan struct{}
	writerDone chan struct{}
	closed     atomic.Bool
	writerUp   atomic.Bool
	startOnce  sync.Once
}

// New creates a VP8 transport backed by the given session.
// The session must not be connected yet when New is called.
func New(session Session, onData func([]byte)) (*Transport, error) {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90000,
		},
		"vp8channel",
		"olcrtc",
	)
	if err != nil {
		return nil, fmt.Errorf("create local vp8 track: %w", err)
	}

	t := &Transport{
		session:    session,
		track:      track,
		onData:     onData,
		outbound:   make(chan []byte, 512),
		closeCh:    make(chan struct{}),
		writerDone: make(chan struct{}),
	}

	if err := session.AddVideoTrack(track); err != nil {
		return nil, fmt.Errorf("attach local vp8 track: %w", err)
	}
	session.SetVideoTrackHandler(t.handleRemoteTrack)

	return t, nil
}

// Connect establishes the underlying session and starts the writer loop.
func (t *Transport) Connect(ctx context.Context) error {
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if err := t.session.Connect(connectCtx); err != nil {
		return err
	}

	t.startOnce.Do(func() {
		t.writerUp.Store(true)
		go t.writerLoop()
	})

	return nil
}

// Send queues data for transmission. Max payload is 60 KB.
// Returns ErrOutboundFull immediately if the queue is full (caller should drop).
func (t *Transport) Send(data []byte) error {
	if t.closed.Load() {
		return ErrTransportClosed
	}

	frame := encodeDataFrame(data)

	select {
	case <-t.closeCh:
		return ErrTransportClosed
	case t.outbound <- frame:
		return nil
	default:
		return ErrOutboundFull
	}
}

// Close terminates the transport and the underlying session.
func (t *Transport) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		close(t.closeCh)
		if t.writerUp.Load() {
			<-t.writerDone
		}
		return t.session.Close()
	}
	return nil
}

// SetReconnectCallback registers a callback invoked after each successful reconnect.
func (t *Transport) SetReconnectCallback(cb func()) {
	t.session.SetReconnectCallback(func(_ *webrtc.DataChannel) {
		if cb != nil {
			cb()
		}
	})
}

// SetShouldReconnect configures whether reconnection is attempted.
func (t *Transport) SetShouldReconnect(fn func() bool) {
	t.session.SetShouldReconnect(fn)
}

// SetEndedCallback registers a callback invoked when the session ends permanently.
func (t *Transport) SetEndedCallback(cb func(string)) {
	t.session.SetEndedCallback(cb)
}

// WatchConnection runs the reconnection loop; call in a goroutine.
func (t *Transport) WatchConnection(ctx context.Context) {
	t.session.WatchConnection(ctx)
}

// CanSend reports whether the transport is ready to send data.
func (t *Transport) CanSend() bool {
	return !t.closed.Load() && t.session.CanSend() && len(t.outbound) < cap(t.outbound)/2
}

// MaxPayloadSize returns the maximum message size this transport supports.
func (t *Transport) MaxPayloadSize() int { return maxPayloadSize }

func (t *Transport) sendFrame(data []byte) {
	_ = t.track.WriteSample(media.Sample{
		Data:     data,
		Duration: sampleDuration,
	})
}

func (t *Transport) writerLoop() {
	defer close(t.writerDone)

	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	// Send a VP8 keyframe every 500 ms so the SFU doesn't freeze the track
	// even when the outbound queue is always busy with data frames.
	const keepaliveEveryTicks = 50
	tickCount := 0

	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			tickCount++
			if tickCount >= keepaliveEveryTicks {
				tickCount = 0
				t.sendFrame(vp8Keepalive)
			}
			t.sendTick()
		}
	}
}

func (t *Transport) sendTick() {
	for i := 0; i < maxFramesPerTick; i++ {
		select {
		case frame := <-t.outbound:
			t.sendFrame(frame)
		default:
			if i == 0 {
				t.sendFrame(vp8Keepalive)
			}
			return
		}
	}
}

func (t *Transport) handleRemoteTrack(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	if track.Codec().MimeType != webrtc.MimeTypeVP8 {
		go t.drainTrack(track)
		return
	}
	go t.readVP8Track(track)
}

func (t *Transport) drainTrack(track *webrtc.TrackRemote) {
	buf := make([]byte, rtpBufSize)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
	}
}

func (t *Transport) readVP8Track(track *webrtc.TrackRemote) {
	var vp8Pkt codecs.VP8Packet
	var frameBuf []byte
	buf := make([]byte, rtpBufSize)

	for {
		n, _, err := track.Read(buf)
		if err != nil {
			return
		}

		pkt := &rtp.Packet{}
		if pkt.Unmarshal(buf[:n]) != nil {
			continue
		}

		vp8Payload, err := vp8Pkt.Unmarshal(pkt.Payload)
		if err != nil {
			continue
		}

		if vp8Pkt.S == 1 {
			frameBuf = frameBuf[:0]
		}
		frameBuf = append(frameBuf, vp8Payload...)

		if pkt.Marker {
			if data := extractDataFromFrame(frameBuf); data != nil && t.onData != nil {
				t.onData(data)
			}
		}
	}
}

func encodeDataFrame(data []byte) []byte {
	frame := make([]byte, 5+len(data))
	frame[0] = dataMarker
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(data)))
	copy(frame[5:], data)
	return frame
}

func extractDataFromFrame(frame []byte) []byte {
	if len(frame) < 5 || frame[0] != dataMarker {
		return nil
	}
	length := binary.BigEndian.Uint32(frame[1:5])
	if uint32(len(frame)) < 5+length {
		return nil
	}
	return frame[5 : 5+length]
}
