package request

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
)

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"http://api.torbox.app/v1/api/torrents/requestdl?token=SECRET&torrent_id=1&file_id=2": "http://api.torbox.app/v1/api/torrents/requestdl",
		"https://user:pass@host/path?a=b#frag":                                                "https://host/path",
		"https://host/path":                                                                   "https://host/path",
		"://not a url":                                                                        "<redacted-url>",
	}
	for in, want := range cases {
		if got := RedactURL(in); got != want {
			t.Errorf("RedactURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactURLErrorPreservesChain(t *testing.T) {
	inner := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	urlErr := &url.Error{
		Op:  "Head",
		URL: "http://127.0.0.1:1/api/torrents/requestdl?token=SUPERSECRETTOKEN&torrent_id=1",
		Err: inner,
	}
	got := RedactURLError(urlErr)
	message := got.Error()
	if strings.Contains(message, "SUPERSECRETTOKEN") || strings.Contains(message, "token=") {
		t.Fatalf("redacted error still carries the token: %s", message)
	}
	if !strings.Contains(message, "127.0.0.1:1/api/torrents/requestdl") {
		t.Fatalf("redacted error lost the host/path diagnostics: %s", message)
	}
	// The unwrap chain must survive so classification still works.
	if !errors.Is(got, syscall.ECONNREFUSED) {
		t.Fatalf("errors.Is lost the underlying cause: %v", got)
	}
	var netErr net.Error
	if !errors.As(got, &netErr) {
		t.Fatalf("errors.As(net.Error) lost the underlying cause: %v", got)
	}
}

func TestRedactURLErrorLeavesOtherErrorsAlone(t *testing.T) {
	plain := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")
	if got := RedactURLError(plain); got != plain {
		t.Fatalf("RedactURLError changed a non-URL error: %v", got)
	}
	if got := RedactURLError(nil); got != nil {
		t.Fatalf("RedactURLError(nil) = %v, want nil", got)
	}
}
