package main

import (
	"context"
	"errors"
	"log"
	"net"
	"strings"

	"github.com/cacggghp/vk-turn-proxy/internal/jazz"
	"github.com/cacggghp/vk-turn-proxy/internal/wbstream"
)

type dataChannelPeer interface {
	Send([]byte) error
	Close() error
}

type dataChannelConnectFunc func(context.Context, string, func([]byte), func()) (dataChannelPeer, error)

func connectJazzDataChannelPeer(ctx context.Context, room string, onData func([]byte), onReconnect func()) (dataChannelPeer, error) {
	return jazz.NewConnectedPeer(ctx, room, onData, onReconnect)
}

func runJazzDataChannelMode(ctx context.Context, room, connectAddr string) error {
	return runDataChannelMode(ctx, "SaluteJazz", connectJazzDataChannelPeer, room, connectAddr)
}

func connectWbstreamDataChannelPeer(ctx context.Context, room string, onData func([]byte), _ func()) (dataChannelPeer, error) {
	parts := strings.Split(room, ",")
	if len(parts) > 1 {
		return wbstream.NewConnectedMultiPeer(ctx, parts, onData)
	}
	return wbstream.NewConnectedPeer(ctx, room, onData)
}

func runWbstreamDataChannelMode(ctx context.Context, room, connectAddr string) error {
	return runDataChannelMode(ctx, "WbStream", connectWbstreamDataChannelPeer, room, connectAddr)
}

func runDataChannelMode(ctx context.Context, providerName string, connectPeer dataChannelConnectFunc, room, connectAddr string) error {
	backendConn, err := net.Dial("udp", connectAddr)
	if err != nil {
		return err
	}
	defer func(backendConn net.Conn) {
		err := backendConn.Close()
		if err != nil {
			log.Println(err)
		}
	}(backendConn)

	peer, err := connectPeer(ctx, room, func(data []byte) {
		if _, writeErr := backendConn.Write(data); writeErr != nil {
			log.Printf("%s DataChannel: failed to write backend packet: %v", providerName, writeErr)
		}
	}, nil)
	if err != nil {
		return err
	}
	defer func(peer dataChannelPeer) {
		err := peer.Close()
		if err != nil {
			log.Println(err)
		}
	}(peer)

	closeOnContextDone(ctx, backendConn)

	log.Printf("%s DataChannel mode: forwarding to %s", providerName, connectAddr)

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
			log.Printf("%s DataChannel: dropped backend packet (%d bytes): %v", providerName, n, err)
		}
	}
}
