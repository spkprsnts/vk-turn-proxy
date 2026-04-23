// Package proxyseq adds sequence numbers to proxy frames and reassembles
// them in order on the receive side. Used by multi-peer mode to compensate
// for out-of-order delivery across independent DataChannel connections.
package proxyseq

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultTimeout is how long the reorder buffer waits for a missing sequence
// before skipping it and delivering later packets.
const DefaultTimeout = 100 * time.Millisecond

// Wrap prepends a 4-byte little-endian sequence number to data.
func Wrap(seq *atomic.Uint32, data []byte) []byte {
	s := seq.Add(1) - 1
	frame := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(frame, s)
	copy(frame[4:], data)
	return frame
}

// Unwrap strips the 4-byte sequence number and returns (seq, payload, ok).
func Unwrap(frame []byte) (uint32, []byte, bool) {
	if len(frame) < 4 {
		return 0, nil, false
	}
	return binary.LittleEndian.Uint32(frame), frame[4:], true
}

// Reorder buffers out-of-order frames and delivers them in sequence.
// If the next expected frame does not arrive within timeout, the buffer
// skips to the next available sequence so downstream is not starved.
type Reorder struct {
	mu      sync.Mutex
	nextSeq uint32
	buf     map[uint32][]byte
	deliver func([]byte)
	timeout time.Duration
	gen     uint64
	timer   *time.Timer
}

// New returns a Reorder that calls deliver for each in-order payload.
func New(deliver func([]byte)) *Reorder {
	return NewWithTimeout(deliver, DefaultTimeout)
}

// NewWithTimeout is like New but lets the caller set a custom gap timeout.
func NewWithTimeout(deliver func([]byte), timeout time.Duration) *Reorder {
	return &Reorder{
		buf:     make(map[uint32][]byte),
		deliver: deliver,
		timeout: timeout,
	}
}

// Push inserts a frame into the buffer and delivers all consecutive frames
// starting from the next expected sequence number.
// Late frames (seq < nextSeq, i.e. after a timeout skip) are delivered
// immediately out-of-order — WireGuard's anti-replay window accepts them.
func (r *Reorder) Push(seq uint32, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)

	r.mu.Lock()
	if seq < r.nextSeq {
		// Late arrival after a gap skip: deliver directly instead of dropping.
		// WireGuard handles out-of-order within its 2048-packet anti-replay window.
		r.mu.Unlock()
		r.deliver(cp)
		return
	}
	r.buf[seq] = cp
	ready := r.flush()
	if len(r.buf) > 0 {
		r.resetTimer()
	} else {
		r.cancelTimer()
	}
	r.mu.Unlock()

	for _, d := range ready {
		r.deliver(d)
	}
}

// flush collects all consecutive frames starting at nextSeq.
// Must be called with r.mu held.
func (r *Reorder) flush() [][]byte {
	var out [][]byte
	for {
		d, ok := r.buf[r.nextSeq]
		if !ok {
			break
		}
		delete(r.buf, r.nextSeq)
		r.nextSeq++
		out = append(out, d)
	}
	return out
}

// resetTimer (re)starts the gap timeout. Must be called with r.mu held.
func (r *Reorder) resetTimer() {
	r.gen++
	gen := r.gen
	if r.timer != nil {
		r.timer.Stop()
	}
	r.timer = time.AfterFunc(r.timeout, func() { r.onTimeout(gen) })
}

// cancelTimer stops any pending timer. Must be called with r.mu held.
func (r *Reorder) cancelTimer() {
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}
}

// onTimeout fires when a gap has persisted past the deadline.
// It skips to the lowest buffered sequence so delivery resumes.
func (r *Reorder) onTimeout(gen uint64) {
	r.mu.Lock()
	if r.gen != gen || len(r.buf) == 0 {
		r.mu.Unlock()
		return
	}

	// Jump to the lowest available sequence.
	var minSeq uint32 = ^uint32(0)
	for seq := range r.buf {
		if seq < minSeq {
			minSeq = seq
		}
	}
	r.nextSeq = minSeq
	ready := r.flush()
	if len(r.buf) > 0 {
		r.resetTimer()
	} else {
		r.timer = nil
	}
	r.mu.Unlock()

	for _, d := range ready {
		r.deliver(d)
	}
}
