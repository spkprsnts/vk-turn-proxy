package main

import (
	"context"
	"errors"
	"strings"

	"github.com/spkprsnts/vk-turn-proxy/internal/jazz"
	"github.com/spkprsnts/vk-turn-proxy/internal/wbstream"
)

func connectJazzPeer(ctx context.Context, room string, onData func([]byte), onReconnect func()) (channelPeer, error) {
	return jazz.NewConnectedPeer(ctx, room, onData, onReconnect)
}

func runJazzMode(ctx context.Context, room, listenAddr string) error {
	return runChannelMode(ctx, "SaluteJazz", connectJazzPeer, room, listenAddr)
}

func runJazzVLESSMode(ctx context.Context, room, listenAddr string) error {
	return runChannelVLESSMode(ctx, "SaluteJazz", connectJazzPeer, room, listenAddr)
}

func connectWbstreamPeer(ctx context.Context, room string, onData func([]byte), _ func()) (channelPeer, error) {
	parts := strings.Split(room, ",")
	if len(parts) > 1 {
		for _, p := range parts {
			if p == "" || p == "any" {
				return nil, errors.New("client requires specific room IDs via -wb-room (no \"any\" allowed)")
			}
		}
		return wbstream.NewConnectedMultiPeer(ctx, parts, onData)
	}
	if room == "" || room == "any" {
		return nil, errors.New("client requires a specific room ID via -wb-room")
	}
	return wbstream.NewConnectedPeer(ctx, room, onData)
}

func runWbstreamMode(ctx context.Context, room, listenAddr string) error {
	return runChannelMode(ctx, "WbStream", connectWbstreamPeer, room, listenAddr)
}

func runWbstreamVLESSMode(ctx context.Context, room, listenAddr string) error {
	return runChannelVLESSMode(ctx, "WbStream", connectWbstreamPeer, room, listenAddr)
}
