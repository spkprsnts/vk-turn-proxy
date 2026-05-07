package main

import (
	"context"
	"errors"
	"log"
	"net"
)

// channelPeer is the common interface for any overlay channel (DataChannel, VP8, etc.).
type channelPeer interface {
	Send([]byte) error
	Close() error
}

// channelConnectFunc creates a connected channelPeer for the given room/URL.
type channelConnectFunc func(ctx context.Context, room string, onData func([]byte), onReconnect func()) (channelPeer, error)

// runChannelMode forwards UDP packets between a channelPeer and a backend UDP address.
func runChannelMode(ctx context.Context, providerName string, connectPeer channelConnectFunc, room, connectAddr string) error {
	backendConn, err := net.Dial("udp", connectAddr)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := backendConn.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()

	peer, err := connectPeer(ctx, room, func(data []byte) {
		if _, writeErr := backendConn.Write(data); writeErr != nil {
			log.Printf("%s: failed to write backend packet: %v", providerName, writeErr)
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

	closeOnContextDone(ctx, backendConn)

	log.Printf("%s: forwarding to %s", providerName, connectAddr)

	buf := make([]byte, 2048)
	for {
		n, err := backendConn.Read(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}

		if err := peer.Send(buf[:n]); err != nil {
			log.Printf("%s: dropped backend packet (%d bytes): %v", providerName, n, err)
		}
	}
}
