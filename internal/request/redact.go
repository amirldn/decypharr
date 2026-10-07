package request

import (
	"errors"
	"fmt"
	"net/url"
)

// RedactURL returns raw with its query, fragment, and userinfo removed so a
// credential-bearing URL (for example a TorBox /requestdl URL whose token
// query parameter is the download-account token) can be logged safely. The
// scheme, host, and path are kept for diagnostics.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<redacted-url>"
	}
	u.RawQuery = ""
	u.ForceQuery = false
	u.Fragment = ""
	u.RawFragment = ""
	u.User = nil
	return u.String()
}

// RedactURLError rewrites a transport error so any URL it embeds — notably the
// *url.Error that wraps an http.Client failure — has its credentials stripped.
// The unwrap chain is preserved, so errors.Is/As and net.Error classification
// keep working on the underlying cause.
func RedactURLError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		inner := urlErr.Unwrap()
		if inner == nil {
			inner = errors.New("request failed")
		}
		return &redactedURLError{op: urlErr.Op, url: RedactURL(urlErr.URL), err: inner}
	}
	return err
}

type redactedURLError struct {
	op  string
	url string
	err error
}

func (e *redactedURLError) Error() string {
	return fmt.Sprintf("%s %q: %s", e.op, e.url, e.err.Error())
}

func (e *redactedURLError) Unwrap() error { return e.err }
