package main

import (
	"context"

	"github.com/spkprsnts/vk-turn-proxy/internal/telemost"
)

func connectTelemostPeer(ctx context.Context, roomURL string, onData func([]byte), onReconnect func()) (channelPeer, error) {
	return telemost.NewConnectedPeer(ctx, roomURL, onData, onReconnect)
}

func runTelemostMode(ctx context.Context, roomURL, listenAddr string) error {
	return runChannelMode(ctx, "Telemost/VP8C", connectTelemostPeer, roomURL, listenAddr)
}

func runTelemostVLESSMode(ctx context.Context, roomURL, listenAddr string) error {
	return runChannelVLESSMode(ctx, "Telemost/VP8C", connectTelemostPeer, roomURL, listenAddr)
}
