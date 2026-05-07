// Package vp8channel provides a best-effort byte transport over a VP8 video track.
// Data is prefixed with a marker byte and length field inside raw VP8 frames;
// keepalive frames carry a minimal valid VP8 key frame so the SFU stays happy.
package vp8channel

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
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
	tickInterval     = 5 * time.Millisecond
	sampleDuration   = 5 * time.Millisecond
	maxFramesPerTick = 64
)

var debugLogging atomic.Bool

func SetDebug(enabled bool) { debugLogging.Store(enabled) }
func debugf(format string, args ...any) {
	if debugLogging.Load() {
		log.Printf(format, args...)
	}
}

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
	closed        atomic.Bool
	writerUp      atomic.Bool
	firstDataSent atomic.Bool
	startOnce     sync.Once
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
		outbound:   make(chan []byte, 64),
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
// Blocks until space is available, providing backpressure to the caller.
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
	if err := t.track.WriteSample(media.Sample{
		Data:     data,
		Duration: sampleDuration,
	}); err != nil {
		log.Printf("VP8 WriteSample error: %v", err)
	}
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
			if t.firstDataSent.CompareAndSwap(false, true) {
				log.Printf("VP8 first data frame sent: %d bytes", len(frame))
			}
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
	firstData := true

	log.Printf("VP8 track receiving: codec=%s", track.Codec().MimeType)

	var statsRTP, statsUnmarshalErr, statsVP8Err, statsS1, statsMarker, statsDelivered uint64
	statsTicker := time.NewTicker(5 * time.Second)
	defer statsTicker.Stop()

	deliver := func() {
		if data := extractDataFromFrame(frameBuf); data != nil {
			statsDelivered++
			if firstData {
				firstData = false
				log.Printf("VP8 first data frame received: %d bytes", len(data))
			}
			if t.onData != nil {
				t.onData(data)
			}
		} else if len(frameBuf) >= 64 && !(len(frameBuf) == len(vp8Keepalive) && frameBuf[0] == vp8Keepalive[0]) {
			// Only log large unexpected frames (small ones are SFU probe video)
			debugf("VP8 deliver: unexpected frame len=%d first=0x%02x", len(frameBuf), frameBuf[0])
		}
	}

	for {
		select {
		case <-statsTicker.C:
			debugf("VP8 rx stats: rtp=%d unmarshalErr=%d vp8Err=%d S1=%d marker=%d delivered=%d frameBufLen=%d",
				statsRTP, statsUnmarshalErr, statsVP8Err, statsS1, statsMarker, statsDelivered, len(frameBuf))
		default:
		}

		n, _, err := track.Read(buf)
		if err != nil {
			debugf("VP8 readVP8Track ended: %v", err)
			return
		}
		statsRTP++

		pkt := &rtp.Packet{}
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			statsUnmarshalErr++
			log.Printf("VP8 RTP Unmarshal error: %v (n=%d)", err, n)
			continue
		}

		vp8Payload, err := vp8Pkt.Unmarshal(pkt.Payload)
		if err != nil {
			statsVP8Err++
			firstByte := byte(0)
			if len(pkt.Payload) > 0 {
				firstByte = pkt.Payload[0]
			}
			log.Printf("VP8 payload Unmarshal error: %v payload_len=%d first=0x%02x", err, len(pkt.Payload), firstByte)
			continue
		}

		if vp8Pkt.S == 1 {
			statsS1++
			// S=1 means start of new frame. If the SFU stripped the Marker bit
			// on the previous frame, deliver it now before discarding the buffer.
			if len(frameBuf) > 0 {
				deliver()
			}
			frameBuf = frameBuf[:0]
		}
		frameBuf = append(frameBuf, vp8Payload...)

		if pkt.Marker {
			statsMarker++
			deliver()
			frameBuf = frameBuf[:0]
		}
	}
}

func encodeDataFrame(data []byte) []byte {
	// Prefix every data frame with a valid VP8 key frame so the SFU's VP8
	// validator passes it. Without the prefix, our 0xFF marker byte makes
	// the frame look like VP8 version 7 (invalid), which the SFU drops.
	kLen := len(vp8Keepalive)
	frame := make([]byte, kLen+5+len(data))
	copy(frame, vp8Keepalive)
	frame[kLen] = dataMarker
	binary.BigEndian.PutUint32(frame[kLen+1:kLen+5], uint32(len(data)))
	copy(frame[kLen+5:], data)
	return frame
}

func extractDataFromFrame(frame []byte) []byte {
	// Skip the VP8 key frame prefix added by encodeDataFrame.
	kLen := len(vp8Keepalive)
	if len(frame) > kLen && bytes.Equal(frame[:kLen], vp8Keepalive) {
		frame = frame[kLen:]
	}
	if len(frame) < 5 || frame[0] != dataMarker {
		return nil
	}
	length := binary.BigEndian.Uint32(frame[1:5])
	if uint32(len(frame)) < 5+length {
		return nil
	}
	return frame[5 : 5+length]
}
