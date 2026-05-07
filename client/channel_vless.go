package main

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/spkprsnts/vk-turn-proxy/internal/streammux"
)

// runChannelVLESSMode multiplexes TCP connections over a channelPeer.
func runChannelVLESSMode(ctx context.Context, providerName string, connectPeer channelConnectFunc, room, listenAddr string) error {
	var (
		connMu sync.Mutex
		conns  = make(map[uint16]net.Conn)
	)
	closeAll := func() {
		connMu.Lock()
		defer connMu.Unlock()
		for sid, conn := range conns {
			_ = conn.Close()
			delete(conns, sid)
		}
	}

	var peer channelPeer
	clientID := uint32(time.Now().UnixNano())
	mux := streammux.New(clientID, func(frame []byte) error {
		return peer.Send(frame)
	})
	peer, err := connectPeer(ctx, room, mux.HandleFrame, func() {
		log.Printf("%s VLESS: peer reconnected, closing active TCP streams", providerName)
		closeAll()
		mux.Reset()
	})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := peer.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()
	closeOnContextDone(ctx, listener)

	log.Printf("%s VLESS: listening on %s", providerName, listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				closeAll()
				return nil
			}
			log.Printf("%s VLESS accept error: %v", providerName, err)
			continue
		}

		sid := mux.OpenStream()
		connMu.Lock()
		conns[sid] = conn
		connMu.Unlock()

		go func(streamID uint16, tcpConn net.Conn) {
			defer func() {
				connMu.Lock()
				delete(conns, streamID)
				connMu.Unlock()
				if err := mux.CloseStream(streamID); err != nil {
					log.Printf("%s VLESS: failed to close mux stream %d: %v", providerName, streamID, err)
				}
				_ = tcpConn.Close()
				mux.CleanupStream(streamID)
			}()

			done := make(chan struct{})
			streamClosed := make(chan struct{})

			go func() {
				defer close(done)
				buf := make([]byte, 32768)
				for {
					n, readErr := tcpConn.Read(buf)
					if readErr != nil {
						return
					}
					if sendErr := mux.SendData(streamID, buf[:n]); sendErr != nil {
						return
					}
				}
			}()

			go func() {
				defer close(streamClosed)
				for {
					dataReady := mux.WaitForData(streamID)

					select {
					case <-ctx.Done():
						return
					case <-done:
						return
					case _, ok := <-dataReady:
						if !ok {
							return
						}
					}

					for {
						data := mux.ReadStream(streamID)
						if len(data) == 0 {
							break
						}
						if _, writeErr := tcpConn.Write(data); writeErr != nil {
							return
						}
					}

					if mux.StreamClosed(streamID) {
						return
					}
				}
			}()

			select {
			case <-ctx.Done():
			case <-done:
			case <-streamClosed:
			}
		}(sid, conn)
	}
}
