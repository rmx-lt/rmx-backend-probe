package records

import (
	"testing"

	"github.com/rmx-lt/rmx-backend-probe/internal/protocol"
)

func event(id int64) protocol.ChangeEvent {
	return protocol.ChangeEvent{V: protocol.EventDelete{ID: id}}
}

func TestHubDeliversInOrderToEverySubscriber(t *testing.T) {
	h := newHub()
	a, b := h.Subscribe(), h.Subscribe()
	h.publish(event(1))
	h.publish(event(2))
	for _, s := range []*Subscription{a, b} {
		if (<-s.C).RecordID() != 1 || (<-s.C).RecordID() != 2 {
			t.Fatal("events out of order")
		}
	}
	a.Close()
	a.Close() // harmless
	if _, open := <-a.C; open {
		t.Fatal("a closed subscription still delivers")
	}
	h.publish(event(3))
	if (<-b.C).RecordID() != 3 {
		t.Fatal("the other subscriber missed an event")
	}
	if a.Lagged() {
		t.Fatal("closing is not lagging")
	}
}

func TestHubDropsSlowSubscribers(t *testing.T) {
	h := newHub()
	slow := h.Subscribe()
	for i := range bufferSize + 1 {
		h.publish(event(int64(i)))
	}
	// What was buffered is still delivered, then the channel closes.
	n := 0
	for range slow.C {
		n++
	}
	if n != bufferSize || !slow.Lagged() {
		t.Fatalf("delivered %d events, lagged=%v", n, slow.Lagged())
	}
	// Publishing never blocks, even with nobody left.
	h.publish(event(0))
	slow.Close()
}
