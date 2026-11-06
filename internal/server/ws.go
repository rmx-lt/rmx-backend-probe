package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/rmx-lt/rmx-backend-probe/internal/auth"
	"github.com/rmx-lt/rmx-backend-probe/internal/clientip"
	"github.com/rmx-lt/rmx-backend-probe/internal/protocol"
	"github.com/rmx-lt/rmx-backend-probe/internal/records"
)

// WebSocket transport at /probe/ws.
//
// Client to server: RequestEnvelope JSON text frames.
// Server to client: ServerMessage JSON, either the response to a request or a
// change event pushed whenever any transport changes a record.

const (
	// pingInterval keeps idle connections alive through proxies and detects dead peers.
	pingInterval = 30 * time.Second
	// maxMessageSize is the largest frame accepted from a client.
	maxMessageSize = 64 * 1024
	writeTimeout   = 10 * time.Second
	// closeTimeout is how long to wait for a client to answer our close frame.
	closeTimeout = 2 * time.Second

	// StatusSessionEnded is the close code sent when the session behind the
	// socket has ended (logout, timeout, password change). The client then
	// shows the login form.
	StatusSessionEnded websocket.StatusCode = 4001
)

// handleWS is only reached with a valid session: RequireSession runs before
// this, and the Origin check before that.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFrom(r.Context())
	s.sockets.Add(1)
	defer s.sockets.Done()
	// The library's own Origin check compares with the Host header, which is
	// wrong behind a proxy; ours (auth.CheckOrigin) has already run.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return // Accept has answered the client
	}
	conn.SetReadLimit(maxMessageSize)
	// Every change over this socket is recorded with the handshake's IP.
	src := records.Source{Via: protocol.ConnWSS, IP: clientip.Of(r)}
	s.serveSocket(conn, user, src)
}

func (s *Server) serveSocket(conn *websocket.Conn, user auth.CurrentUser, src records.Source) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer conn.CloseNow()

	events := s.records.Subscribe()
	defer events.Close()
	slog.Debug("websocket connected", "username", user.Username)
	defer slog.Debug("websocket disconnected")

	// A reader goroutine, so pings can be answered and a close noticed while a request runs.
	texts := make(chan []byte)
	readFailed := make(chan error, 1)
	go func() {
		for {
			kind, data, err := conn.Read(ctx)
			if err != nil {
				readFailed <- err
				return
			}
			if kind != websocket.MessageText {
				continue // binary frames are ignored
			}
			select {
			case texts <- data:
			case <-ctx.Done():
				return
			}
		}
	}()

	send := func(msg protocol.ServerMessage) bool {
		body, err := json.Marshal(msg)
		if err != nil {
			slog.Error("encoding a websocket message failed", "error", err)
			return false
		}
		wctx, wcancel := context.WithTimeout(ctx, writeTimeout)
		defer wcancel()
		return conn.Write(wctx, websocket.MessageText, body) == nil
	}

	ping := time.NewTicker(pingInterval)
	defer ping.Stop()
	for {
		select {
		case text := <-texts:
			response := s.respond(ctx, src, text)
			if !send(protocol.ServerMessage{V: protocol.MsgResponse{Response: response}}) {
				return
			}

		case event, ok := <-events.C:
			if !ok {
				if events.Lagged() {
					// The client misses some events. Closing makes it reconnect and list again.
					slog.Warn("websocket client lagged, closing")
					closeWithin(conn, cancel, websocket.StatusTryAgainLater, "lagged, reconnect")
				}
				return
			}
			if !send(protocol.ServerMessage{V: protocol.MsgEvent{Event: event}}) {
				return
			}

		case <-ping.C:
			// A socket stays open for hours, so the session is checked again
			// with every ping: logging out (anywhere) or a timeout closes it
			// within pingInterval.
			if u, err := s.auth.Validate(ctx, user.SessionHash, false); err != nil || u == nil {
				closeWithin(conn, cancel, StatusSessionEnded, "session ended")
				return
			}
			pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
			err := conn.Ping(pctx)
			pcancel()
			if err != nil {
				return
			}

		case err := <-readFailed:
			slog.Debug("websocket read ended", "error", err)
			return

		case <-s.ctx.Done():
			closeWithin(conn, cancel, websocket.StatusGoingAway, "server shutting down")
			return
		}
	}
}

func (s *Server) respond(ctx context.Context, src records.Source, text []byte) protocol.ResponseEnvelope {
	env, failure := ParseRequest(text)
	if failure != nil {
		return *failure
	}
	return protocol.ResponseEnvelope{RequestID: env.RequestID, Result: s.records.Handle(ctx, src, env.Request)}
}

// ParseRequest parses a RequestEnvelope. On failure it returns the error
// response to send, keeping the request_id if the message had one. Shared with MQTT.
func ParseRequest(payload []byte) (protocol.RequestEnvelope, *protocol.ResponseEnvelope) {
	var env protocol.RequestEnvelope
	err := protocol.Unmarshal(payload, &env)
	if err == nil {
		return env, nil
	}
	var partial struct {
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(payload, &partial)
	return env, &protocol.ResponseEnvelope{
		RequestID: partial.RequestID,
		Result: protocol.RpcResult{V: protocol.RpcError{Error: protocol.ApiError{
			Code:    protocol.CodeBadRequest,
			Message: fmt.Sprintf("invalid request: %v", err),
		}}},
	}
}

// closeWithin sends a close frame and waits for the client's answer, but not
// longer than closeTimeout: a client that has stopped reading must not hold up
// a shutdown. Then cancel (the context of the reader goroutine) drops the connection.
func closeWithin(conn *websocket.Conn, cancel context.CancelFunc, code websocket.StatusCode, reason string) {
	done := make(chan struct{})
	go func() {
		_ = conn.Close(code, reason)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeTimeout):
		cancel()
		<-done
	}
}
