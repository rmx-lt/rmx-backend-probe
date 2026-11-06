package server

import (
	"testing"

	"github.com/rmx-lt/rmx-backend-probe/internal/protocol"
)

func TestInvalidRequestKeepsRequestID(t *testing.T) {
	_, failure := ParseRequest([]byte(`{"request_id":"r9","request":{"op":"nope"}}`))
	if failure == nil || failure.RequestID != "r9" {
		t.Fatalf("%+v", failure)
	}
	if e, ok := failure.Result.V.(protocol.RpcError); !ok || e.Error.Code != protocol.CodeBadRequest {
		t.Fatalf("%+v", failure.Result)
	}

	_, failure = ParseRequest([]byte("not json"))
	if failure == nil || failure.RequestID != "" {
		t.Fatalf("%+v", failure)
	}

	env, failure := ParseRequest([]byte(`{"request_id":"r1","request":{"op":"list"}}`))
	if failure != nil || env.RequestID != "r1" {
		t.Fatalf("%+v %+v", env, failure)
	}
}
