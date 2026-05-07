package main

import (
	"context"
	"strings"

	"github.com/cacggghp/vk-turn-proxy/internal/jazz"
	"github.com/cacggghp/vk-turn-proxy/internal/wbstream"
)

func connectJazzPeer(ctx context.Context, room string, onData func([]byte), onReconnect func()) (channelPeer, error) {
	return jazz.NewConnectedPeer(ctx, room, onData, onReconnect)
}

func runJazzMode(ctx context.Context, room, connectAddr string) error {
	return runChannelMode(ctx, "SaluteJazz", connectJazzPeer, room, connectAddr)
}

func runJazzVLESSMode(ctx context.Context, room, connectAddr string) error {
	return runChannelVLESSMode(ctx, "SaluteJazz", connectJazzPeer, room, connectAddr)
}

func connectWbstreamPeer(ctx context.Context, room string, onData func([]byte), _ func()) (channelPeer, error) {
	parts := strings.Split(room, ",")
	if len(parts) > 1 {
		return wbstream.NewConnectedMultiPeer(ctx, parts, onData)
	}
	return wbstream.NewConnectedPeer(ctx, room, onData)
}

func runWbstreamMode(ctx context.Context, room, connectAddr string) error {
	return runChannelMode(ctx, "WbStream", connectWbstreamPeer, room, connectAddr)
}

func runWbstreamVLESSMode(ctx context.Context, room, connectAddr string) error {
	return runChannelVLESSMode(ctx, "WbStream", connectWbstreamPeer, room, connectAddr)
}
