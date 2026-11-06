package clientip

import (
	"net/http"
	"testing"
)

func TestFromProxyHeader(t *testing.T) {
	const peer = "172.18.0.2:50000"
	h := func(lines ...string) http.Header {
		hdr := http.Header{}
		for _, l := range lines {
			hdr.Add("X-Forwarded-For", l)
		}
		return hdr
	}
	for _, c := range []struct {
		hdr  http.Header
		want string
	}{
		{h(), "172.18.0.2"},
		{h("203.0.113.7"), "203.0.113.7"},
		// The last entry is the one the nearest proxy added.
		{h("1.2.3.4, 203.0.113.7"), "203.0.113.7"},
		{h("1.2.3.4", "203.0.113.7"), "203.0.113.7"},
		{h("2001:db8::1"), "2001:db8::1"},
		{h("nope"), "172.18.0.2"},
	} {
		if got := FromHeaders(c.hdr, peer); got.String() != c.want {
			t.Errorf("%v: got %s, want %s", c.hdr, got, c.want)
		}
	}
	if got := FromHeaders(h(), "[::ffff:10.0.0.1]:80"); got.String() != "10.0.0.1" {
		t.Error(got)
	}
	if got := FromHeaders(h(), "garbage"); got.String() != "0.0.0.0" {
		t.Error(got)
	}
}
