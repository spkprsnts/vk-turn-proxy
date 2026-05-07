package telemost

import (
	"context"
	"log"

	"github.com/spkprsnts/vk-turn-proxy/internal/namegen"
	"github.com/spkprsnts/vk-turn-proxy/internal/vp8channel"
)

// NewConnectedPeer creates a Telemost peer wrapped in a VP8 channel transport,
// connects it, and starts the reconnect watcher in a goroutine.
// roomURL is a full Telemost conference URL (e.g. https://telemost.yandex.ru/j/...).
// onReconnect is called after each successful reconnection; may be nil.
func NewConnectedPeer(ctx context.Context, roomURL string, onData func([]byte), onReconnect func()) (*vp8channel.Transport, error) {
	peer, err := NewPeer(ctx, roomURL, namegen.Generate(), nil)
	if err != nil {
		return nil, err
	}

	tr, err := vp8channel.New(peer, onData)
	if err != nil {
		_ = peer.Close()
		return nil, err
	}

	if onReconnect != nil {
		tr.SetReconnectCallback(onReconnect)
	}

	if err := tr.Connect(ctx); err != nil {
		if closeErr := tr.Close(); closeErr != nil {
			log.Printf("Telemost VP8 peer cleanup failed: %v", closeErr)
		}
		return nil, err
	}

	go tr.WatchConnection(ctx)

	return tr, nil
}
