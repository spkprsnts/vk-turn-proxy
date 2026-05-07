package main

import (
	"context"

	"github.com/spkprsnts/vk-turn-proxy/internal/telemost"
)

func connectTelemostPeer(ctx context.Context, roomURL string, onData func([]byte), onReconnect func()) (channelPeer, error) {
	return telemost.NewConnectedPeer(ctx, roomURL, onData, onReconnect)
}

func runTelemostMode(ctx context.Context, roomURL, connectAddr string) error {
	return runChannelMode(ctx, "Telemost/VP8C", connectTelemostPeer, roomURL, connectAddr)
}

func runTelemostVLESSMode(ctx context.Context, roomURL, connectAddr string) error {
	return runChannelVLESSMode(ctx, "Telemost/VP8C", connectTelemostPeer, roomURL, connectAddr)
}
