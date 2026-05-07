package main

import (
	"context"
	"errors"
	"log"
	"net"
	"sync/atomic"
)

// channelPeer is the common interface for any overlay channel (DataChannel, VP8, etc.).
type channelPeer interface {
	Send([]byte) error
	Close() error
}

// channelConnectFunc creates a connected channelPeer for the given room/URL.
type channelConnectFunc func(ctx context.Context, room string, onData func([]byte), onReconnect func()) (channelPeer, error)

// runChannelMode forwards UDP packets between a local listener and a channelPeer.
func runChannelMode(ctx context.Context, providerName string, connectPeer channelConnectFunc, room, listenAddr string) error {
	listenConn, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := listenConn.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()

	var activeLocalPeer atomic.Value
	peer, err := connectPeer(ctx, room, func(data []byte) {
		addr, ok := activeLocalPeer.Load().(net.Addr)
		if !ok || addr == nil {
			return
		}
		if _, writeErr := listenConn.WriteTo(data, addr); writeErr != nil {
			log.Printf("%s: failed to write local packet: %v", providerName, writeErr)
		}
	}, nil)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := peer.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()

	closeOnContextDone(ctx, listenConn)

	log.Printf("%s: listening on %s", providerName, listenAddr)

	buf := make([]byte, 2048)
	for {
		n, addr, err := listenConn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		activeLocalPeer.Store(addr)
		if err := peer.Send(buf[:n]); err != nil {
			log.Printf("%s: dropped outbound packet (%d bytes): %v", providerName, n, err)
		}
	}
}
