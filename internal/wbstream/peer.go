// Package wbstream implements the WbStream WebRTC provider via LiveKit.
package wbstream

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
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

const wsURL = "wss://rtc-el-01.wb.ru:"

var (
	ErrPeerClosed    = errors.New("peer closed")
	ErrSendQueueFull = errors.New("send queue full")
)

// Peer represents a WbStream WebRTC connection using LiveKit.
type Peer struct {
	roomURL   string
	name      string
	room      *lksdk.Room
	onData    func([]byte)
	onEnded   func(string)
	sendQueue chan []byte
	closed    atomic.Bool
	done      chan struct{}
	cancel    context.CancelFunc
	wg        sync.WaitGroup
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
		log.Printf("WbStream: to join use -wb-room %s", roomID)
	}

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
