// Package wbstream implements the WbStream WebRTC provider via LiveKit.
package wbstream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"sync/atomic"

	"github.com/cacggghp/vk-turn-proxy/internal/namegen"
	"github.com/go-logr/stdr"
	protoLogger "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

var debugLogging atomic.Bool

func SetDebug(enabled bool) {
	debugLogging.Store(enabled)
	if enabled {
		lksdk.SetLogger(protoLogger.LogRLogger(stdr.New(log.Default())))
	} else {
		lksdk.SetLogger(protoLogger.GetDiscardLogger())
	}
}

const wsURL = "wss://rtc-el-01.wb.ru"

var (
	ErrPeerClosed    = errors.New("peer closed")
	ErrSendQueueFull = errors.New("send queue full")
)

// Peer represents a WbStream WebRTC connection using LiveKit.
type Peer struct {
	roomURL        string
	roomID         string
	name           string
	room           *lksdk.Room
	onData         func([]byte)
	onEnded        func(string)
	sendQueue      chan []byte
	closed         atomic.Bool
	done           chan struct{}
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	suppressJoinLog bool
}

// NewPeer creates a new WbStream peer (not yet connected).
func NewPeer(ctx context.Context, roomURL, name string, onData func([]byte)) (*Peer, error) {
	_, cancel := context.WithCancel(ctx)
	return &Peer{
		roomURL:   roomURL,
		name:      name,
		onData:    onData,
		sendQueue: make(chan []byte, 5000),
		done:      make(chan struct{}),
		cancel:    cancel,
	}, nil
}

// NewConnectedPeer creates and connects a WbStream peer.
func NewConnectedPeer(ctx context.Context, roomURL string, onData func([]byte)) (*Peer, error) {
	peer, err := NewPeer(ctx, roomURL, namegen.Generate(), onData)
	if err != nil {
		return nil, err
	}
	if err := peer.Connect(ctx); err != nil {
		return nil, err
	}
	return peer, nil
}

type roomCredentials struct {
	token     string
	serverURL string
}

// Connect starts the WebRTC connection process.
func (p *Peer) Connect(ctx context.Context) error {
	creds, err := p.getRoomCredentials(ctx)
	if err != nil {
		return fmt.Errorf("get room credentials: %w", err)
	}

	serverURL := creds.serverURL
	if serverURL == "" {
		serverURL = wsURL
		log.Printf("WbStream: no serverUrl in token response, using fallback %s", wsURL)
	}

	roomCB := &lksdk.RoomCallback{
		ParticipantCallback: lksdk.ParticipantCallback{
			OnDataReceived: func(data []byte, _ lksdk.DataReceiveParams) {
				if p.onData != nil {
					p.onData(data)
				}
			},
		},
		OnDisconnected: func() {
			log.Printf("WbStream DataChannel closed")
			if p.onEnded != nil {
				p.onEnded("disconnected from livekit")
			}
		},
	}

	connectOpts := []lksdk.ConnectOption{lksdk.WithAutoSubscribe(true)}
	if !debugLogging.Load() {
		connectOpts = append(connectOpts, lksdk.WithLogger(protoLogger.GetDiscardLogger()))
	}
	room, err := lksdk.ConnectToRoomWithToken(serverURL, creds.token, roomCB, connectOpts...)
	if err != nil {
		return fmt.Errorf("connect to room: %w", err)
	}

	p.room = room
	log.Printf("WbStream DataChannel connected")
	p.wg.Add(1)
	go p.processSendQueue()

	return nil
}

func (p *Peer) getRoomCredentials(ctx context.Context) (roomCredentials, error) {
	accessToken, err := registerGuest(ctx, p.name)
	if err != nil {
		return roomCredentials{}, fmt.Errorf("register guest: %w", err)
	}

	roomID := p.roomURL
	if roomID == "" || roomID == "any" {
		roomID, err = createRoom(ctx, accessToken)
		if err != nil {
			return roomCredentials{}, fmt.Errorf("create room: %w", err)
		}
		log.Printf("WbStream: room created: %s", roomID)
		if !p.suppressJoinLog {
			log.Printf("WbStream: to join use -wb-room %s", roomID)
		}
	}
	p.roomID = roomID

	log.Printf("WbStream joining room: %s", roomID)
	if err := joinRoom(ctx, accessToken, roomID); err != nil {
		return roomCredentials{}, fmt.Errorf("join room: %w", err)
	}

	res, err := getToken(ctx, accessToken, roomID, p.name)
	if err != nil {
		return roomCredentials{}, fmt.Errorf("get token: %w", err)
	}

	serverURL := res.ServerURL
	if serverURL == "" {
		serverURL = res.WsURL
	}
	if serverURL == "" {
		serverURL = res.LiveKitURL
	}
	if serverURL == "" {
		serverURL = res.LiveKitWsURL
	}

	return roomCredentials{token: res.RoomToken, serverURL: serverURL}, nil
}

func (p *Peer) processSendQueue() {
	defer p.wg.Done()
	for {
		select {
		case <-p.done:
			return
		case data, ok := <-p.sendQueue:
			if !ok {
				return
			}
			if err := p.room.LocalParticipant.PublishDataPacket(
				lksdk.UserData(data),
				lksdk.WithDataPublishTopic("vk-turn-proxy"),
				lksdk.WithDataPublishReliable(true),
			); err != nil {
				log.Printf("WbStream: publish data error: %v", err)
			}
		}
	}
}

// Send transmits data to the room.
func (p *Peer) Send(data []byte) error {
	if p.closed.Load() {
		return ErrPeerClosed
	}
	select {
	case p.sendQueue <- data:
		return nil
	default:
		return ErrSendQueueFull
	}
}

// Close terminates the connection.
func (p *Peer) Close() error {
	if p.closed.CompareAndSwap(false, true) {
		log.Printf("WbStream DataChannel closed")
		p.cancel()
		close(p.done)
		if p.room != nil {
			p.room.Disconnect()
		}
		close(p.sendQueue)
		p.wg.Wait()
	}
	return nil
}

// SetEndedCallback sets the callback invoked when the session ends.
func (p *Peer) SetEndedCallback(cb func(string)) {
	p.onEnded = cb
}

// RoomID returns the actual room ID used by this peer.
func (p *Peer) RoomID() string {
	return p.roomID
}

// MultiPeer stripes traffic round-robin across N parallel Peer connections.
type MultiPeer struct {
	peers   []*Peer
	sendIdx atomic.Uint64
}

var multiPeerRetryDelays = []time.Duration{3 * time.Second, 8 * time.Second, 15 * time.Second}

// NewConnectedMultiPeer creates one Peer per roomURL and returns a MultiPeer.
// Each peer is retried up to len(multiPeerRetryDelays) times on failure.
// After all peers connect it logs the combined room IDs for use with -wb-room.
func NewConnectedMultiPeer(ctx context.Context, roomURLs []string, onData func([]byte)) (*MultiPeer, error) {
	mp := &MultiPeer{
		peers: make([]*Peer, 0, len(roomURLs)),
	}
	cleanup := func() {
		for _, already := range mp.peers {
			_ = already.Close()
		}
	}

	for i, url := range roomURLs {
		if i > 0 {
			select {
			case <-ctx.Done():
				cleanup()
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}

		var p *Peer
		var lastErr error
		for attempt := 0; attempt <= len(multiPeerRetryDelays); attempt++ {
			if attempt > 0 {
				delay := multiPeerRetryDelays[attempt-1]
				log.Printf("WbStream: peer %d/%d: attempt %d failed (%v), retrying in %v...", i+1, len(roomURLs), attempt, lastErr, delay)
				select {
				case <-ctx.Done():
					cleanup()
					return nil, ctx.Err()
				case <-time.After(delay):
				}
			}
			peer, err := NewPeer(ctx, url, namegen.Generate(), onData)
			if err != nil {
				lastErr = err
				continue
			}
			peer.suppressJoinLog = true
			if err := peer.Connect(ctx); err != nil {
				lastErr = err
				continue
			}
			p = peer
			break
		}
		if p == nil {
			cleanup()
			return nil, fmt.Errorf("peer %d/%d: all attempts failed: %w", i+1, len(roomURLs), lastErr)
		}
		mp.peers = append(mp.peers, p)
	}

	ids := make([]string, len(mp.peers))
	for i, p := range mp.peers {
		ids[i] = p.RoomID()
	}
	log.Printf("WbStream MultiPeer (%d connections) ready. Client: -wb-room %s", len(mp.peers), strings.Join(ids, ","))
	return mp, nil
}

// Send transmits data through the next peer in round-robin order.
func (mp *MultiPeer) Send(data []byte) error {
	n := uint64(len(mp.peers))
	idx := mp.sendIdx.Add(1) - 1
	return mp.peers[idx%n].Send(data)
}

// Close closes all underlying peers.
func (mp *MultiPeer) Close() error {
	var first error
	for _, p := range mp.peers {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// SetEndedCallback sets the disconnection callback on all underlying peers.
func (mp *MultiPeer) SetEndedCallback(cb func(string)) {
	for _, p := range mp.peers {
		p.SetEndedCallback(cb)
	}
}
