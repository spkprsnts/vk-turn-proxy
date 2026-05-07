// Package vp8channel provides reliable, ordered byte transport over a VP8 video track.
// Data rides inside VP8 frames that carry KCP segments; the KCP ARQ layer
// provides retransmission and in-order delivery on top of the lossy SFU path.
// An epoch header in every frame lets each side detect a peer restart and
// reset its KCP session so both ends converge on fresh state.
package vp8channel

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spkprsnts/vk-turn-proxy/internal/logger"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

const (
	maxPayloadSize       = 60 * 1024
	connectTimeout       = 60 * time.Second
	rtpBufSize           = 65536
	outboundQueueSize = 256 // 256 × ~1129-byte segments ≈ 289 KB; ~113 ms burst at 20 Mbps
	inboundQueueSize  = 256 // same sizing; larger than outbound avoids drop-triggered retransmits
	canSendHighWatermark = 90 // percent of outbound queue
	keepaliveIdlePeriod  = 100 * time.Millisecond
	tickInterval         = 5 * time.Millisecond
)

var (
	ErrTransportClosed = errors.New("vp8channel transport closed")
	ErrOutboundFull    = errors.New("vp8channel outbound full")

	// vp8Keepalive is a minimal valid 16×16 VP8 key frame sent as idle heartbeat
	// so the SFU keeps the track alive between data bursts.
	vp8Keepalive = []byte{
		0x30, 0x01, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00,
		0x10, 0x00, 0x00, 0x47, 0x08, 0x85, 0x85, 0x88,
		0x99, 0x84, 0x88, 0xfc,
	}
)

// Wire layout of a KCP data frame (after VP8 RTP descriptor is stripped):
//
//	[0..19]  = vp8Keepalive (valid VP8 key frame so Telemost SFU forwards it)
//	[20]     = kcpFrameMagic (0x4B = 'K')
//	[21..24] = proxyBindingToken (big-endian uint32)
//	[25..28] = sender's session epoch (big-endian uint32)
//	[29..]   = raw KCP packet bytes
//
// Telemost SFU only forwards VP8 key frames (bit0=0 in the first byte).
// Prefixing every KCP frame with a valid 16×16 VP8 key frame (vp8Keepalive)
// ensures the SFU always treats our frames as key frames and forwards them.
// Pure keepalives are exactly keepalivePrefixLen bytes; KCP frames are longer.
const (
	kcpFrameMagic = byte(0x4B) // 'K'

	// proxyBindingToken identifies frames belonging to this proxy protocol.
	// Both client and server use the same constant so they accept each other's
	// frames while rejecting VP8 from unrelated room participants.
	proxyBindingToken = uint32(0x564B5250) // "VKRP"

	keepalivePrefixLen = 20 // len(vp8Keepalive)

	kcpMagicOff = keepalivePrefixLen     // 20
	tokenOff    = keepalivePrefixLen + 1 // 21
	epochOff    = keepalivePrefixLen + 5 // 25
	epochHdrLen = keepalivePrefixLen + 9 // 29
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

// Transport carries arbitrary byte messages over a WebRTC VP8 video track
// with reliable, ordered delivery via KCP ARQ.
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
	kcpOnce    sync.Once

	// localEpoch is a per-process random identifier stamped into every
	// outgoing VP8 frame. peerEpoch tracks the last epoch received from the
	// remote so we can detect their restart and reset KCP.
	localEpoch uint32
	peerEpoch  atomic.Uint32
	hadPeer    atomic.Bool

	kcp         *kcpRuntime
	kcpMu       sync.RWMutex
	reconnectMu sync.Mutex
	reconnectFn func()
}

// SetDebug enables or disables verbose debug logging for vp8channel.
func SetDebug(enabled bool) { logger.SetVerbose(enabled) }

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
		outbound:   make(chan []byte, outboundQueueSize),
		closeCh:    make(chan struct{}),
		writerDone: make(chan struct{}),
		localEpoch: randomEpoch(),
	}

	if err := session.AddVideoTrack(track); err != nil {
		return nil, fmt.Errorf("attach local vp8 track: %w", err)
	}
	session.SetVideoTrackHandler(t.handleRemoteTrack)

	return t, nil
}

// Connect establishes the underlying session, starts the writer loop, and
// initialises the KCP session. Call once; subsequent calls are no-ops.
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

	var startErr error
	t.kcpOnce.Do(func() {
		rt, err := startKCP(t.outbound, t.onData, t.epochHeader())
		if err != nil {
			startErr = err
			return
		}
		t.kcpMu.Lock()
		t.kcp = rt
		t.kcpMu.Unlock()
	})
	return startErr
}

// Send queues data for reliable delivery via KCP. Blocks until KCP accepts
// the write; returns ErrTransportClosed if the transport has been shut down.
func (t *Transport) Send(data []byte) error {
	if t.closed.Load() {
		return ErrTransportClosed
	}
	t.kcpMu.RLock()
	rt := t.kcp
	t.kcpMu.RUnlock()
	if rt == nil {
		return ErrTransportClosed
	}
	return rt.send(data)
}

// Close terminates the transport and the underlying session.
func (t *Transport) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		close(t.closeCh)

		t.kcpMu.RLock()
		rt := t.kcp
		t.kcpMu.RUnlock()
		if rt != nil {
			rt.close()
		}

		if t.writerUp.Load() {
			<-t.writerDone
		}
		return t.session.Close()
	}
	return nil
}

// SetReconnectCallback registers a callback invoked after each successful
// reconnect. The KCP session is reset before the callback fires.
func (t *Transport) SetReconnectCallback(cb func()) {
	t.reconnectMu.Lock()
	t.reconnectFn = cb
	t.reconnectMu.Unlock()
	t.session.SetReconnectCallback(func(_ *webrtc.DataChannel) {
		t.resetKCP()
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
// Returns false when the outbound queue is above 90% capacity.
func (t *Transport) CanSend() bool {
	return !t.closed.Load() && t.session.CanSend() &&
		len(t.outbound) < cap(t.outbound)*canSendHighWatermark/100
}

// MaxPayloadSize returns the maximum application message size this transport supports.
func (t *Transport) MaxPayloadSize() int { return maxPayloadSize }

// epochHeader builds the 29-byte VP8-frame header for the current local session.
// It starts with vp8Keepalive so the SFU treats every KCP frame as a key frame.
func (t *Transport) epochHeader() [epochHdrLen]byte {
	var hdr [epochHdrLen]byte
	copy(hdr[:keepalivePrefixLen], vp8Keepalive)
	hdr[kcpMagicOff] = kcpFrameMagic
	binary.BigEndian.PutUint32(hdr[tokenOff:epochOff], proxyBindingToken)
	binary.BigEndian.PutUint32(hdr[epochOff:], t.localEpoch)
	return hdr
}

func randomEpoch() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		//nolint:gosec
		return uint32(time.Now().UnixNano())
	}
	e := binary.BigEndian.Uint32(b[:])
	if e == 0 {
		e = 1
	}
	return e
}

func (t *Transport) drainOutbound() {
	for {
		select {
		case <-t.outbound:
		default:
			return
		}
	}
}

func (t *Transport) resetKCP() {
	t.drainOutbound()
	t.kcpMu.Lock()
	old := t.kcp
	t.kcp = nil
	t.kcpMu.Unlock()
	if old != nil {
		old.close()
	}
	// localEpoch is intentionally NOT bumped here. Bumping it on every
	// peer-triggered reset would cause both sides to loop forever tearing
	// down KCP (each new epoch looks "new" to the other side).
	rt, err := startKCP(t.outbound, t.onData, t.epochHeader())
	if err != nil {
		log.Printf("VP8 resetKCP failed: %v", err)
		return
	}
	t.kcpMu.Lock()
	t.kcp = rt
	t.kcpMu.Unlock()
}

func (t *Transport) writerLoop() {
	defer close(t.writerDone)

	// Fire keepalive when the channel has been idle for keepaliveIdlePeriod.
	// Reset the ticker every time real KCP data is flushed so keepalives are
	// only sent during true idle gaps.
	keepaliveTick := time.NewTicker(keepaliveIdlePeriod)
	defer keepaliveTick.Stop()

	for {
		select {
		case <-t.closeCh:
			return
		case frame := <-t.outbound:
			// Drain ALL queued KCP frames without sleeping between them.
			// Previously one frame per 5 ms tick was the bottleneck.
			t.flushOutbound(frame)
			keepaliveTick.Reset(keepaliveIdlePeriod)
		case <-keepaliveTick.C:
			_ = t.track.WriteSample(media.Sample{
				Data:     vp8Keepalive,
				Duration: tickInterval,
			})
		}
	}
}

func (t *Transport) flushOutbound(first []byte) {
	writeSample := func(frame []byte) {
		if err := t.track.WriteSample(media.Sample{
			Data:     frame,
			Duration: tickInterval,
		}); err != nil {
			logger.Debugf("VP8 WriteSample error len=%d: %v", len(frame), err)
		} else {
			logger.Debugf("VP8 sent KCP frame len=%d", len(frame))
		}
	}
	writeSample(first)
	for {
		select {
		case frame := <-t.outbound:
			writeSample(frame)
		default:
			return
		}
	}
}

func (t *Transport) handleRemoteTrack(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
	if track.Codec().MimeType != webrtc.MimeTypeVP8 {
		go t.drainTrack(track)
		return
	}
	// Peer restarts are detected by the epoch header on incoming frames,
	// which works even when the SFU keeps forwarding the same track object.
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

// vp8FrameState accumulates RTP packets into complete VP8 frames.
type vp8FrameState struct {
	vp8Pkt      codecs.VP8Packet
	frameBuf    []byte
	lastSeq     uint16
	haveLastSeq bool
	frameValid  bool
}

// processRTPPacket returns a complete VP8 frame when fully assembled, nil
// otherwise. Tracks sequence numbers to discard frames corrupted by packet loss.
func (s *vp8FrameState) processRTPPacket(pkt *rtp.Packet) []byte {
	if s.haveLastSeq && pkt.SequenceNumber != s.lastSeq+1 {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
	}
	s.lastSeq = pkt.SequenceNumber
	s.haveLastSeq = true

	vp8Payload, err := s.vp8Pkt.Unmarshal(pkt.Payload)
	if err != nil {
		s.frameValid = false
		s.frameBuf = s.frameBuf[:0]
		return nil
	}

	if s.vp8Pkt.S == 1 {
		s.frameBuf = s.frameBuf[:0]
		s.frameValid = true
	}

	if !s.frameValid {
		return nil
	}

	s.frameBuf = append(s.frameBuf, vp8Payload...)

	if !pkt.Marker {
		return nil
	}

	defer func() {
		s.frameBuf = s.frameBuf[:0]
		s.frameValid = false
	}()

	// KCP frames are prefixed with vp8Keepalive (20 B) followed by kcpFrameMagic.
	// Pure keepalives are exactly keepalivePrefixLen bytes and are silently dropped.
	if len(s.frameBuf) >= epochHdrLen && s.frameBuf[kcpMagicOff] == kcpFrameMagic {
		frame := make([]byte, len(s.frameBuf))
		copy(frame, s.frameBuf)
		return frame
	}
	if len(s.frameBuf) != keepalivePrefixLen {
		logger.Debugf("VP8 frame assembled but not KCP: len=%d first=0x%02x", len(s.frameBuf), s.frameBuf[0])
	}
	return nil
}

func (t *Transport) readVP8Track(track *webrtc.TrackRemote) {
	var state vp8FrameState
	buf := make([]byte, rtpBufSize)

	log.Printf("VP8 track receiving: codec=%s", track.Codec().MimeType)

	for {
		n, _, err := track.Read(buf)
		if err != nil {
			logger.Debugf("VP8 readVP8Track ended: %v", err)
			return
		}

		pkt := &rtp.Packet{}
		if pkt.Unmarshal(buf[:n]) != nil {
			continue
		}

		frame := state.processRTPPacket(pkt)
		if frame == nil {
			continue
		}

		t.handleIncomingFrame(frame)
	}
}

// handleIncomingFrame parses the epoch header, filters by binding token,
// and either delivers the KCP payload to the local session or resets KCP
// when the peer's epoch changes (peer process restart detected).
func (t *Transport) handleIncomingFrame(frame []byte) {
	if len(frame) < epochHdrLen {
		return
	}
	tok := binary.BigEndian.Uint32(frame[tokenOff:epochOff])
	if tok != proxyBindingToken {
		logger.Debugf("VP8 frame rejected: token=%08x (want %08x)", tok, proxyBindingToken)
		return
	}
	peerEpoch := binary.BigEndian.Uint32(frame[epochOff:epochHdrLen])
	kcpPayload := frame[epochHdrLen:]
	if len(kcpPayload) == 0 {
		return
	}
	// Some SFUs reflect our own published VP8 track back as a remote track.
	// Those frames carry our localEpoch — skip them to avoid the epoch-change
	// detector toggling between "self" and "peer" and looping forever.
	if peerEpoch == t.localEpoch {
		logger.Debugf("VP8 frame rejected: self-reflection epoch=%08x", peerEpoch)
		return
	}

	if !t.hadPeer.Swap(true) {
		t.peerEpoch.Store(peerEpoch)
		log.Printf("VP8 channel established (peer epoch=%08x)", peerEpoch)
	} else if prev := t.peerEpoch.Load(); prev != peerEpoch {
		// Peer restarted its KCP session. CAS guards against double-reset
		// when fragmented frames straddle the epoch boundary.
		if t.peerEpoch.CompareAndSwap(prev, peerEpoch) {
			log.Printf("VP8 peer epoch changed %08x→%08x, resetting KCP", prev, peerEpoch)
			t.resetKCP()
			t.reconnectMu.Lock()
			fn := t.reconnectFn
			t.reconnectMu.Unlock()
			if fn != nil {
				fn()
			}
		}
		// Drop packet — it predates our fresh KCP session.
		return
	}

	t.kcpMu.RLock()
	rt := t.kcp
	t.kcpMu.RUnlock()
	if rt != nil {
		logger.Debugf("VP8 delivering KCP frame len=%d epoch=%08x", len(kcpPayload), peerEpoch)
		rt.deliver(kcpPayload)
	}
}
