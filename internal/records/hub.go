package records

import (
	"sync"

	"github.com/rmx-lt/rmx-backend-probe/internal/protocol"
)

// bufferSize: a subscriber that falls more than this many events behind is
// dropped and has to resync by listing again.
const bufferSize = 256

// Hub broadcasts every change to all subscribers.
type Hub struct {
	mu   sync.Mutex
	subs map[*Subscription]struct{}
}

// Subscription receives the events published after Subscribe.
type Subscription struct {
	// C delivers events in order. It is closed when the subscription ends: after
	// Close, or when the subscriber was too slow (then Lagged is true). Events
	// already buffered are still delivered before the close shows.
	C <-chan protocol.ChangeEvent

	ch     chan protocol.ChangeEvent
	hub    *Hub
	lagged bool // protected by hub.mu
}

func newHub() *Hub { return &Hub{subs: map[*Subscription]struct{}{}} }

// Subscribe starts receiving events.
func (h *Hub) Subscribe() *Subscription {
	ch := make(chan protocol.ChangeEvent, bufferSize)
	s := &Subscription{C: ch, ch: ch, hub: h}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Close ends the subscription. It is safe to call more than once.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	if _, ok := s.hub.subs[s]; ok {
		delete(s.hub.subs, s)
		close(s.ch)
	}
}

// Lagged reports whether the subscription ended because the subscriber missed
// events. Call it after C is closed.
func (s *Subscription) Lagged() bool {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()
	return s.lagged
}

// publish never blocks: a subscriber with a full buffer is dropped.
func (h *Hub) publish(e protocol.ChangeEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for s := range h.subs {
		select {
		case s.ch <- e:
		default:
			s.lagged = true
			delete(h.subs, s)
			close(s.ch)
		}
	}
}
