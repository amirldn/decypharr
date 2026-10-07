package manager

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
)

// dribbleReader serves chunk-sized reads, each blocked for delay. remaining
// < 0 means infinite; remaining == 0 returns EOF.
type dribbleReader struct {
	chunk     []byte
	delay     time.Duration
	remaining int
	closed    bool
}

func (d *dribbleReader) Read(p []byte) (int, error) {
	if d.remaining == 0 {
		return 0, io.EOF
	}
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	if d.remaining > 0 {
		d.remaining--
	}
	return copy(p, d.chunk), nil
}

func (d *dribbleReader) Close() error { d.closed = true; return nil }

// errReader blocks for delay, then fails every Read with err — a stand-in
// for a body whose context the session's stall watchdog just cancelled.
type errReader struct {
	delay time.Duration
	err   error
}

func (e *errReader) Read(p []byte) (int, error) {
	if e.delay > 0 {
		time.Sleep(e.delay)
	}
	return 0, e.err
}

func (e *errReader) Close() error { return nil }

// healthyThenErrorReader serves good healthy reads, then fails every Read
// with err — a stand-in for a transient error on a healthy stream.
type healthyThenErrorReader struct {
	chunk []byte
	delay time.Duration
	good  int // healthy reads before the error starts
	err   error
	reads int
}

func (h *healthyThenErrorReader) Read(p []byte) (int, error) {
	h.reads++
	if h.reads > h.good {
		return 0, h.err
	}
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	return copy(p, h.chunk), nil
}

func (h *healthyThenErrorReader) Close() error { return nil }

func TestSlowWatchBodyTripsOnSustainedSlowStream(t *testing.T) {
	// 100 bytes per 100ms => ~1 kB/s, far below the 100 kB/s test threshold.
	// Two 150ms windows of blocked read time trip the watchdog.
	var firstWindowBps float64
	var firstWindowCalls int
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: make([]byte, 100), delay: 100 * time.Millisecond, remaining: -1},
		"cdn1.torbox.app",
		func(bps float64) { firstWindowCalls++; firstWindowBps = bps },
		100*1024, 150*time.Millisecond, 2,
	)
	buf := make([]byte, 4096)
	for {
		n, err := w.Read(buf)
		if err == nil {
			if n == 0 {
				t.Fatal("nil error with zero bytes")
			}
			continue
		}
		var serr *link.SlowStreamError
		if !errors.As(err, &serr) {
			t.Fatalf("expected *link.SlowStreamError, got %T (%v)", err, err)
		}
		if n != 0 {
			t.Fatalf("trip must drop the in-flight bytes, got n=%d", n)
		}
		if serr.Host != "cdn1.torbox.app" {
			t.Errorf("host not carried: %q", serr.Host)
		}
		if serr.Bps >= 100*1024 {
			t.Errorf("measured bps implausible: %f", serr.Bps)
		}
		if serr.Bytes <= 0 {
			t.Error("expected some bytes counted before the trip")
		}
		if lerr := link.GetLinkError(err); lerr == nil || !lerr.ShouldRefetch() {
			t.Error("tripped error must classify as refetchable")
		}
		break
	}
	if firstWindowCalls != 1 {
		t.Errorf("onFirstWindow called %d times, want 1", firstWindowCalls)
	}
	if firstWindowBps <= 0 || firstWindowBps >= 100*1024 {
		t.Errorf("first-window bps implausible: %f", firstWindowBps)
	}
}

func TestSlowWatchBodyHealthyStreamNoTrip(t *testing.T) {
	// ~10 MB/s: 100 kB per 10ms, 20 chunks then EOF.
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: make([]byte, 100*1024), delay: 10 * time.Millisecond, remaining: 20},
		"cdn1.torbox.app", nil,
		100*1024, 50*time.Millisecond, 2,
	)
	buf := make([]byte, 256*1024)
	var total int
	for {
		n, err := w.Read(buf)
		total += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("healthy stream must not trip: %v", err)
		}
	}
	if total != 20*100*1024 {
		t.Errorf("short read: got %d bytes", total)
	}
}

func TestSlowWatchBodyConsumerPausesDontCount(t *testing.T) {
	// Reads return instantly (no blocked time); long pauses between them must
	// not accumulate toward a window, so no trip is possible.
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: make([]byte, 100), remaining: 10},
		"cdn1.torbox.app", nil,
		100*1024, 100*time.Millisecond, 2,
	)
	buf := make([]byte, 4096)
	var total int
	for i := 0; i < 10; i++ {
		n, err := w.Read(buf)
		total += n
		if err != nil {
			t.Fatalf("consumer-paced reads must not trip the watchdog: %v", err)
		}
		time.Sleep(150 * time.Millisecond) // pauses are not blocked Read time
	}
	if total != 1000 {
		t.Errorf("got %d bytes, want 1000", total)
	}
}

func TestSlowWatchBodyRecoveryAfterBadWindow(t *testing.T) {
	// One bad window followed by a fast window resets the consecutive count:
	// a single slow patch must not trip the watchdog (it needs badWindows
	// *consecutive* slow windows).
	dr := &dribbleReader{chunk: make([]byte, 100), delay: 60 * time.Millisecond, remaining: -1}
	w := newSlowWatchBodyWithParams(dr, "cdn1.torbox.app", nil,
		100*1024, 50*time.Millisecond, 2)
	buf := make([]byte, 4096)

	// Phase 1: one slow read completes exactly one bad window (~1.6 kB/s).
	if _, err := w.Read(buf); err != nil {
		t.Fatalf("single bad window must not trip: %v", err)
	}
	// Phase 2: fast reads (~10 MB/s) complete a healthy window and reset the
	// consecutive-bad counter.
	dr.chunk = make([]byte, 100*1024)
	dr.delay = 10 * time.Millisecond
	for i := 0; i < 6; i++ {
		if _, err := w.Read(buf); err != nil {
			t.Fatalf("fast reads must not trip: %v", err)
		}
	}
	// Phase 3: slow again — still only one consecutive bad window, no trip.
	dr.chunk = make([]byte, 100)
	dr.delay = 60 * time.Millisecond
	if _, err := w.Read(buf); err != nil {
		t.Fatalf("isolated bad window after recovery must not trip: %v", err)
	}
}

// TestSlowWatchBodyHardStallTrips: a single Read blocked for multiple
// windows with zero bytes flowing trips the watchdog — whether the Read
// returns on its own or the session's stall watchdog cancels it first —
// instead of riding a same-link retry.
func TestSlowWatchBodyHardStallTrips(t *testing.T) {
	w := newSlowWatchBodyWithParams(
		&dribbleReader{chunk: nil, delay: 250 * time.Millisecond, remaining: -1},
		"cdn1.torbox.app", nil,
		100*1024, 100*time.Millisecond, 2,
	)
	start := time.Now()
	_, err := w.Read(make([]byte, 4096))
	elapsed := time.Since(start)
	var serr *link.SlowStreamError
	if !errors.As(err, &serr) {
		t.Fatalf("expected *link.SlowStreamError, got %T (%v)", err, err)
	}
	// The trip is evaluated when the blocked Read returns (~250ms here):
	// two full windows elapsed inside the single Read, so it trips on the
	// first return instead of needing a second Read.
	if elapsed > 400*time.Millisecond {
		t.Errorf("trip took %s, want ~250ms (the blocked read duration)", elapsed)
	}
}

// TestSlowWatchBodyStallCancelledReadTrips: a Read killed by the session's
// stall watchdog (body context cancelled after 90s with zero bytes) carries
// genuine degradation signal — 90s of zero-byte blocked time. The watchdog
// evaluates it like any other Read and trips, so the host is cooled and the
// link swapped instead of riding the same-link retry.
func TestSlowWatchBodyStallCancelledReadTrips(t *testing.T) {
	w := newSlowWatchBodyWithParams(
		&errReader{delay: 250 * time.Millisecond, err: context.Canceled},
		"cdn1.torbox.app", nil,
		100*1024, 100*time.Millisecond, 2,
	)
	n, err := w.Read(make([]byte, 4096))
	var serr *link.SlowStreamError
	if !errors.As(err, &serr) {
		t.Fatalf("expected *link.SlowStreamError, got %T (%v)", err, err)
	}
	if n != 0 {
		t.Fatalf("trip must drop the in-flight bytes, got n=%d", n)
	}
	if serr.Host != "cdn1.torbox.app" {
		t.Errorf("host not carried: %q", serr.Host)
	}
	if lerr := link.GetLinkError(err); lerr == nil || !lerr.ShouldRefetch() {
		t.Error("tripped error must classify as refetchable")
	}
}

// TestSlowWatchBodyErrorPassthroughNoTrip: a transient error on an otherwise
// healthy stream passes through untouched — same n, same error, no trip.
// The trip is bps-gated, so errors with good throughput behind them never
// swap the link.
func TestSlowWatchBodyErrorPassthroughNoTrip(t *testing.T) {
	boom := errors.New("boom")
	w := newSlowWatchBodyWithParams(
		&healthyThenErrorReader{
			chunk: make([]byte, 100*1024), delay: 10 * time.Millisecond,
			good: 5, err: boom,
		},
		"cdn1.torbox.app", nil,
		100*1024, 50*time.Millisecond, 2,
	)
	buf := make([]byte, 256*1024)
	for i := 0; i < 5; i++ {
		if _, err := w.Read(buf); err != nil {
			t.Fatalf("healthy read %d must not error: %v", i, err)
		}
	}
	n, err := w.Read(buf)
	if err != boom {
		t.Fatalf("transient error must pass through untouched, got %T (%v)", err, err)
	}
	if n != 0 {
		t.Fatalf("passthrough must preserve n, got %d", n)
	}
}

// healthyThenSlowErrorReader serves `good` healthy reads, then fails every
// Read with err after blocking for errDelay — a stand-in for a connection
// reset that arrives after a long stall on an otherwise healthy stream.
type healthyThenSlowErrorReader struct {
	chunk    []byte
	delay    time.Duration
	good     int // healthy reads before the error starts
	errDelay time.Duration
	err      error
	reads    int
}

func (h *healthyThenSlowErrorReader) Read(p []byte) (int, error) {
	h.reads++
	if h.reads > h.good {
		if h.errDelay > 0 {
			time.Sleep(h.errDelay)
		}
		return 0, h.err
	}
	if h.delay > 0 {
		time.Sleep(h.delay)
	}
	return copy(p, h.chunk), nil
}

func (h *healthyThenSlowErrorReader) Close() error { return nil }

// TestSlowWatchBodyLongBlockedErrorOnHealthyStreamNoTrip pins the
// highest-risk boundary: a single errored Read blocked for nearly two full
// windows (~180ms of zero-byte blocked time) on a healthy stream must NOT
// trip the watchdog. The windows' bps is an average over the accumulated
// blocked time and bytes (pro-rata carryover), so the healthy bytes keep the
// average far above the threshold — the trip is gated on measured
// throughput, never on the error itself. The original error passes through
// with identity. (The mirror case — the same blocked duration with no
// healthy bytes behind it — does trip; see
// TestSlowWatchBodyStallCancelledReadTrips.)
func TestSlowWatchBodyLongBlockedErrorOnHealthyStreamNoTrip(t *testing.T) {
	boom := errors.New("boom")
	// 400 kB in ~20ms ~= 20 MB/s: one healthy read, then a single errored
	// read blocked ~180ms (nearly two 100ms windows) with zero bytes. Even
	// with generous scheduling overshoot the window average stays far above
	// the 100 kB/s threshold.
	w := newSlowWatchBodyWithParams(
		&healthyThenSlowErrorReader{
			chunk: make([]byte, 400*1024), delay: 20 * time.Millisecond,
			good: 1, errDelay: 180 * time.Millisecond, err: boom,
		},
		"cdn1.torbox.app", nil,
		100*1024, 100*time.Millisecond, 2,
	)
	buf := make([]byte, 512*1024)
	if _, err := w.Read(buf); err != nil {
		t.Fatalf("healthy read must not trip: %v", err)
	}
	n, err := w.Read(buf)
	if err != boom {
		t.Fatalf("long-blocked error on a healthy stream must pass through untouched, got %T (%v)", err, err)
	}
	if n != 0 {
		t.Fatalf("passthrough must preserve n, got %d", n)
	}
	var serr *link.SlowStreamError
	if errors.As(err, &serr) {
		t.Fatalf("healthy stream with a long-blocked errored read must not trip the watchdog: %v", serr)
	}
}

// TestHttpTransportSlowStreamRecover wires the full trip path: the watchdog
// error reaches recover, cools the CDN host, refreshes the link, and arms
// the before/after report for the replacement body.
func TestHttpTransportSlowStreamRecover(t *testing.T) {
	var notedHost string
	var notedBps float64
	var refreshes int
	tr := &httpTransport{
		client: http.DefaultClient,
		noteSlow: func(host string, bps float64) {
			notedHost, notedBps = host, bps
		},
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			refreshes++
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn9.torbox.app/x"}, nil
		},
	}
	tr.last = types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}

	serr := link.NewSlowStreamError("cdn1.torbox.app", 1<<20, 22*1024)
	if err := tr.recover(context.Background(), serr, 0); err != nil {
		t.Fatalf("recover should succeed, got %v", err)
	}
	if notedHost != "cdn1.torbox.app" || notedBps != 22*1024 {
		t.Errorf("noteSlow got host=%q bps=%f", notedHost, notedBps)
	}
	if refreshes != 1 {
		t.Errorf("expected exactly one refresh, got %d", refreshes)
	}
	tr.mu.Lock()
	report := tr.swapArmed
	tr.mu.Unlock()
	if report == nil || report.oldHost != "cdn1.torbox.app" || report.beforeBps != 22*1024 {
		t.Fatalf("swap report not armed: %+v", report)
	}
}

// TestHttpTransportSlowStreamRecoverNilHooks ensures transports built without
// the slow-stream hooks (e.g. existing unit tests) still recover fine.
func TestHttpTransportSlowStreamRecoverNilHooks(t *testing.T) {
	tr := &httpTransport{
		client: http.DefaultClient,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn9.torbox.app/x"}, nil
		},
	}
	tr.last = types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}
	if err := tr.recover(context.Background(), link.NewSlowStreamError("cdn1.torbox.app", 1, 1), 0); err != nil {
		t.Fatalf("recover with nil hooks should succeed, got %v", err)
	}
}

// TestWatchBodyHostFromFinalURL opens through a real 302 redirect chain and
// asserts the watchdog measures the FINAL host — the CDN node serving the
// bytes — not the link URL's host.
func TestWatchBodyHostFromFinalURL(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4")
		_, _ = w.Write([]byte("data"))
	}))
	defer cdn.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, cdn.URL+"/f", http.StatusFound)
	}))
	defer redirector.Close()

	tr := &httpTransport{
		client: redirector.Client(), // follows redirects by default
		getLink: func(context.Context) (types.DownloadLink, error) {
			// The link URL names the redirector; the bytes come from cdn.
			return types.DownloadLink{Filename: "f", DownloadLink: redirector.URL + "/f"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: redirector.URL + "/f"}, nil
		},
	}

	body, err := tr.open(context.Background(), 0)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer body.Close()
	w, ok := body.(*slowWatchBody)
	if !ok {
		t.Fatalf("open should return a *slowWatchBody, got %T", body)
	}
	u, _ := url.Parse(cdn.URL)
	if w.host != u.Host {
		t.Errorf("watchdog host = %q, want final CDN host %q (link host was the redirector)", w.host, u.Host)
	}
	if _, err := io.ReadAll(w); err != nil {
		t.Fatalf("read failed: %v", err)
	}
}

// TestHttpTransportSlowStreamRecoverClearsArmedOnRefreshFailure: a failed
// swap must not leave the report armed for the next open.
func TestHttpTransportSlowStreamRecoverClearsArmedOnRefreshFailure(t *testing.T) {
	tr := &httpTransport{
		client: http.DefaultClient,
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			return types.DownloadLink{}, errors.New("provider exploded")
		},
	}
	tr.last = types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}
	err := tr.recover(context.Background(), link.NewSlowStreamError("cdn1.torbox.app", 1, 1), 0)
	if err == nil {
		t.Fatal("refresh failure should propagate")
	}
	tr.mu.Lock()
	armed := tr.swapArmed
	tr.mu.Unlock()
	if armed != nil {
		t.Errorf("failed swap left a stale armed report: %+v", armed)
	}
}

// TestHttpTransportSlowStreamRecoverClearsArmedOnEmptyBadLink: when there is
// no link to invalidate (tr.last is the zero value) recover takes the
// bad.Empty() exit — the armed report must still be cleared so a later open
// cannot inherit this swap's old_host, and no refresh is attempted.
func TestHttpTransportSlowStreamRecoverClearsArmedOnEmptyBadLink(t *testing.T) {
	tr := &httpTransport{
		client:   http.DefaultClient,
		noteSlow: func(host string, bps float64) {},
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			t.Fatal("refresh must not run when there is no bad link to invalidate")
			return types.DownloadLink{}, nil
		},
	}
	// tr.last left as the zero value: bad.Empty() is true.
	if err := tr.recover(context.Background(), link.NewSlowStreamError("cdn1.torbox.app", 1, 1), 0); err != nil {
		t.Fatalf("recover with an empty bad link should succeed, got %v", err)
	}
	tr.mu.Lock()
	armed := tr.swapArmed
	tr.mu.Unlock()
	if armed != nil {
		t.Errorf("empty bad link left a stale armed report: %+v", armed)
	}
}

// errRoundTripper fails every request — a stand-in for a CDN that is
// unreachable right after a slow-stream swap.
type errRoundTripper struct{ err error }

func (e errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, e.err
}

// TestHttpTransportOpenFailureClearsArmedReport: when the replacement open
// fails after recover armed a slow-stream report, the report must be
// dropped. The swap's replacement body never opened, and the failed open is
// followed by recovery — the next successful open may serve a different link
// entirely, so consuming the stale report there would emit a
// slow_stream_after pairing the old swap's old_host/before_kbps with an
// unrelated body.
func TestHttpTransportOpenFailureClearsArmedReport(t *testing.T) {
	tr := &httpTransport{
		client: &http.Client{Transport: errRoundTripper{err: errors.New("connection refused")}},
		getLink: func(context.Context) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}, nil
		},
		refresh: func(_ context.Context, bad types.DownloadLink) (types.DownloadLink, error) {
			return types.DownloadLink{Filename: "f", DownloadLink: "https://cdn9.torbox.app/x"}, nil
		},
	}
	tr.last = types.DownloadLink{Filename: "f", DownloadLink: "https://cdn1.torbox.app/x"}

	// Arm the report via a slow-stream trip with a successful swap.
	if err := tr.recover(context.Background(), link.NewSlowStreamError("cdn1.torbox.app", 1, 1), 0); err != nil {
		t.Fatalf("recover should succeed, got %v", err)
	}
	tr.mu.Lock()
	if tr.swapArmed == nil {
		tr.mu.Unlock()
		t.Fatal("recover should have armed the swap report")
	}
	tr.mu.Unlock()

	// The replacement open fails: the armed report must be dropped, not
	// left for a later open to inherit.
	if _, err := tr.open(context.Background(), 0); err == nil {
		t.Fatal("open should fail against the broken transport")
	}
	tr.mu.Lock()
	armed := tr.swapArmed
	tr.mu.Unlock()
	if armed != nil {
		t.Errorf("failed open left a stale armed report: %+v", armed)
	}
}
