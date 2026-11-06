// Package protocol defines the wire types shared by probe-server and the
// browser client.
//
// Everything here is plain JSON over the wire. The same request and response
// envelopes are used on WSS and MQTT, so a client can switch transports
// without changing its message handling. The TypeScript copies in
// client/src/generated are generated from these types by cmd/gen-ts
// (run `go run ./cmd/gen-ts`).
//
// Types that are a union in TypeScript (Request, RpcResult, ...) are structs
// holding one variant in V; see union.go.
package protocol

import (
	"fmt"
	"log/slog"
	"time"
	"unicode/utf8"
)

// MaxValueChars is the longest accepted value, in characters.
const MaxValueChars = 1000

// ConnectionType is the transport that created or last changed a record.
type ConnectionType string

const (
	ConnHTTPS ConnectionType = "https"
	ConnWSS   ConnectionType = "wss"
	ConnMQTT  ConnectionType = "mqtt"
)

// RecordOp is the kind of change logged in record_events.
type RecordOp string

const (
	OpCreate RecordOp = "create"
	OpUpdate RecordOp = "update"
	OpDelete RecordOp = "delete"
)

// ErrorCode says what went wrong, in an ApiError.
type ErrorCode string

const (
	CodeBadRequest ErrorCode = "bad_request"
	CodeNotFound   ErrorCode = "not_found"
	CodeInternal   ErrorCode = "internal"
	// CodeUnauthorized: no valid session (REST: 401), or wrong username or password at login.
	CodeUnauthorized ErrorCode = "unauthorized"
	// CodeForbidden: the request came from a page on another origin (REST: 403).
	CodeForbidden ErrorCode = "forbidden"
	// CodeTooManyRequests: too many failed logins; wait Retry-After seconds (REST: 429).
	CodeTooManyRequests ErrorCode = "too_many_requests"
)

// RecordRow is one row of the records table.
type RecordRow struct {
	ID int64 `json:"id"`
	// When the record was created.
	Time           time.Time      `json:"time"`
	ConnectionType ConnectionType `json:"connection_type"`
	Value          *string        `json:"value"`
	UpdatedAt      time.Time      `json:"updated_at"`
	// The client IP that created the record (an update keeps it). Null for
	// records created over MQTT: the broker doesn't pass on the client's
	// address, so the server can't know it.
	CreatedIP *string `json:"created_ip"`
}

// RecordInput is the body of POST /probe/api/records and PUT /probe/api/records/{id}.
type RecordInput struct {
	Value *string `json:"value"`
}

// Validate checks the input. The error message is suitable for the client.
func (in RecordInput) Validate() error {
	if in.Value != nil && utf8.RuneCountInString(*in.Value) > MaxValueChars {
		return fmt.Errorf("value is longer than %d characters", MaxValueChars)
	}
	return nil
}

// ---- Requests (client to server, over WSS and MQTT) -------------------------

// RequestList lists the newest records.
type RequestList struct{}

// RequestCreate creates a record.
type RequestCreate struct {
	Value *string `json:"value"`
}

// RequestUpdate changes the value of a record.
type RequestUpdate struct {
	ID    int64   `json:"id"`
	Value *string `json:"value"`
}

// RequestDelete deletes a record.
type RequestDelete struct {
	ID int64 `json:"id"`
}

// Request is a CRUD operation, sent by the client over WSS or MQTT.
type Request struct {
	// V is a RequestList, RequestCreate, RequestUpdate or RequestDelete.
	V any
}

// RequestEnvelope is a request plus the id the client uses to match the response.
type RequestEnvelope struct {
	RequestID string  `json:"request_id"`
	Request   Request `json:"request"`
}

// ---- Responses --------------------------------------------------------------

// ApiError is the error body, used by REST (with a matching HTTP status) and
// in RpcError.
type ApiError struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
}

// RpcRecords answers a list.
type RpcRecords struct {
	Records []RecordRow `json:"records"`
}

// RpcRecord answers a create or update.
type RpcRecord struct {
	Record RecordRow `json:"record"`
}

// RpcDeleted answers a delete.
type RpcDeleted struct {
	ID int64 `json:"id"`
}

// RpcData is the payload of a successful answer.
type RpcData struct {
	// V is an RpcRecords, RpcRecord or RpcDeleted.
	V any
}

// RpcOk is a successful result.
type RpcOk struct {
	Data RpcData `json:"data"`
}

// RpcError is a failed result.
type RpcError struct {
	Error ApiError `json:"error"`
}

// RpcResult is the outcome of one request.
type RpcResult struct {
	// V is an RpcOk or RpcError.
	V any
}

// ResponseEnvelope is the answer to one RequestEnvelope.
type ResponseEnvelope struct {
	// Copied from the request. Empty if the request could not be parsed at all.
	RequestID string    `json:"request_id"`
	Result    RpcResult `json:"result"`
}

// ---- Events -----------------------------------------------------------------

// EventCreate: a record was created.
type EventCreate struct {
	Record RecordRow `json:"record"`
}

// EventUpdate: a record was changed.
type EventUpdate struct {
	Record RecordRow `json:"record"`
}

// EventDelete: a record was deleted.
type EventDelete struct {
	ID             int64          `json:"id"`
	ConnectionType ConnectionType `json:"connection_type"`
}

// ChangeEvent is a change to records, broadcast to every client whatever
// transport caused it. Sent on WSS (inside a ServerMessage) and on the MQTT events
// topic.
type ChangeEvent struct {
	// V is an EventCreate, EventUpdate or EventDelete.
	V any
}

// RecordID is the id of the record the event is about.
func (e ChangeEvent) RecordID() int64 {
	switch v := e.V.(type) {
	case EventCreate:
		return v.Record.ID
	case EventUpdate:
		return v.Record.ID
	case EventDelete:
		return v.ID
	}
	return 0
}

// MsgResponse is the answer to a request.
type MsgResponse struct {
	Response ResponseEnvelope `json:"response"`
}

// MsgEvent is a pushed change.
type MsgEvent struct {
	Event ChangeEvent `json:"event"`
}

// ServerMessage is every message the server sends on the WebSocket.
type ServerMessage struct {
	// V is a MsgResponse or MsgEvent.
	V any
}

// ---- Plain REST bodies ------------------------------------------------------

// Health is the body of GET /probe/health.
type Health struct {
	// "ok" when the database and the MQTT broker are both reachable, else "degraded".
	Status  string `json:"status"`
	DB      bool   `json:"db"`
	MQTT    bool   `json:"mqtt"`
	Version string `json:"version"`
}

// LoginRequest is the body of POST /probe/api/auth/login.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// String and LogValue keep the password out of log lines.
func (r LoginRequest) String() string { return fmt.Sprintf("LoginRequest{Username: %q}", r.Username) }

// GoString is what %#v prints.
func (r LoginRequest) GoString() string { return r.String() }

// LogValue is what slog prints.
func (r LoginRequest) LogValue() slog.Value {
	return slog.GroupValue(slog.String("username", r.Username))
}

// Me is the logged-in user: the answer to a login and to GET /probe/api/auth/me.
type Me struct {
	Username string `json:"username"`
}
