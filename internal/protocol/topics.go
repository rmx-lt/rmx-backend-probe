package protocol

import "strings"

// MQTT topic layout. A client puts its own id in the topic, like mowers will
// (mower/<id>/...), so a broker ACL can scope each login later.
//
//   - <prefix>/req/<client-id>: client to server, payload RequestEnvelope
//   - <prefix>/res/<client-id>: server to client, payload ResponseEnvelope
//   - <prefix>/events/records: server to all, payload ChangeEvent
//   - <prefix>/status/server: retained "online" / "offline" (last will)

// RequestTopic is where a client publishes its requests.
func RequestTopic(prefix, clientID string) string { return prefix + "/req/" + clientID }

// RequestFilter is a subscription filter that matches every client's requests.
func RequestFilter(prefix string) string { return prefix + "/req/+" }

// ResponseTopic is where the server answers a client.
func ResponseTopic(prefix, clientID string) string { return prefix + "/res/" + clientID }

// EventsTopic is where the server publishes every change.
func EventsTopic(prefix string) string { return prefix + "/events/records" }

// StatusTopic carries the retained "online" / "offline" of the server.
func StatusTopic(prefix string) string { return prefix + "/status/server" }

// ParseRequestTopic extracts the client id from a request topic. It returns
// false for any other topic, or for an id that is not a single plain topic level.
func ParseRequestTopic(prefix, topic string) (string, bool) {
	rest, ok := strings.CutPrefix(topic, prefix)
	if !ok {
		return "", false
	}
	id, ok := strings.CutPrefix(rest, "/req/")
	if !ok || !ValidClientID(id) {
		return "", false
	}
	return id, true
}

// ValidClientID reports whether id is 1 to 64 characters of [A-Za-z0-9_-], so
// the id is safe inside a topic.
func ValidClientID(id string) bool {
	if len(id) < 1 || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}
