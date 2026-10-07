package torbox

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

func TestSubmitMagnetAcceptsDocumentedObjectResponse(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"detail":"Torrent Added Successfully","data":{"torrent_id":41,"hash":"AABBCC"}}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	got, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err != nil {
		t.Fatalf("SubmitMagnet() error = %v", err)
	}
	if got.Id != "41" {
		t.Fatalf("SubmitMagnet() id = %q, want 41", got.Id)
	}
}

func TestSubmitMagnetSelectsMatchingTorrentFromArrayResponse(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"detail":"Torrent Added Successfully","data":[{"id":7,"hash":"UNRELATED"},{"id":42,"hash":"aabbcc"}]}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	got, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err != nil {
		t.Fatalf("SubmitMagnet() error = %v", err)
	}
	if got.Id != "42" {
		t.Fatalf("SubmitMagnet() id = %q, want matching torrent 42", got.Id)
	}
}

func TestSubmitMagnetRejectsArrayWithoutMatchingHash(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"detail":"Torrent Added Successfully","data":[{"id":7,"hash":"UNRELATED"}]}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err == nil || !strings.Contains(err.Error(), "no torrent matching submitted hash") {
		t.Fatalf("SubmitMagnet() error = %v, want safe no-match error", err)
	}
}

func TestSubmitMagnetIncludesSanitizedProviderError(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, `{"success":false,"error":"BAD_REQUEST","detail":"magnet:?xt=urn:btih:SECRET is invalid","data":null}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := testTorbox(server.URL).SubmitMagnet(torrent)
	if err == nil {
		t.Fatal("SubmitMagnet() error = nil, want provider error")
	}
	message := err.Error()
	if !strings.Contains(message, "BAD_REQUEST") || !strings.Contains(message, "[redacted magnet]") {
		t.Fatalf("SubmitMagnet() error = %q, want sanitized provider fields", message)
	}
	if strings.Contains(message, "SECRET") || strings.Contains(message, "magnet:?") {
		t.Fatalf("SubmitMagnet() error leaked submitted magnet data: %q", message)
	}
}

func TestSubmitMagnetPreservesStatusAndBodyOnExhaustedRetries(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprint(w, `{"success":false,"error":"RATE_LIMITED","detail":"Rate limit exceeded","data":null}`)
	}))
	t.Cleanup(server.Close)

	tb := &Torbox{
		Host: server.URL,
		client: request.New(
			request.WithMaxRetries(2),
			request.WithRetryWait(time.Millisecond, 2*time.Millisecond),
			request.WithRetryableStatus(http.StatusTooManyRequests),
		),
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "torbox"},
	}

	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := tb.SubmitMagnet(torrent)
	if err == nil {
		t.Fatal("SubmitMagnet() error = nil, want rate limit error after retries")
	}
	message := err.Error()
	if !strings.Contains(message, "Status: 429") {
		t.Fatalf("SubmitMagnet() error = %q, want Status: 429", message)
	}
	if !strings.Contains(message, "RATE_LIMITED") || !strings.Contains(message, "Rate limit exceeded") {
		t.Fatalf("SubmitMagnet() error = %q, want provider error details", message)
	}
	if !strings.Contains(message, "attempts=3") {
		t.Fatalf("SubmitMagnet() error = %q, want attempts=3", message)
	}
	if !strings.Contains(message, "retry-after=\"5\"") {
		t.Fatalf("SubmitMagnet() error = %q, want retry-after=\"5\"", message)
	}
	if strings.Contains(message, "giving up after") {
		t.Fatalf("SubmitMagnet() error = %q, should not contain retryablehttp generic giving up message", message)
	}
	if attempts != 3 {
		t.Fatalf("server received %d attempts, want 3", attempts)
	}
}

func TestSubmitMagnetHandlesChunkedErrorResponse(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if flusher, ok := w.(http.Flusher); ok {
			_, _ = fmt.Fprint(w, `{"success":false,"error":"BAD_REQUEST","detail":"chunked error body"}`)
			flusher.Flush()
		} else {
			_, _ = fmt.Fprint(w, `{"success":false,"error":"BAD_REQUEST","detail":"chunked error body"}`)
		}
	}))
	t.Cleanup(server.Close)

	tb := testTorbox(server.URL)
	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := tb.SubmitMagnet(torrent)
	if err == nil {
		t.Fatal("SubmitMagnet() error = nil, want error")
	}
	message := err.Error()
	if !strings.Contains(message, "BAD_REQUEST") || !strings.Contains(message, "chunked error body") {
		t.Fatalf("SubmitMagnet() error = %q, want chunked provider details", message)
	}
}

func TestSubmitMagnetHandlesEmptyBodyErrorResponse(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)

	tb := testTorbox(server.URL)
	torrent := &debridTypes.Torrent{
		InfoHash:         "AABBCC",
		DownloadUncached: true,
		Magnet:           &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"},
	}
	_, err := tb.SubmitMagnet(torrent)
	if err == nil {
		t.Fatal("SubmitMagnet() error = nil, want error")
	}
	message := err.Error()
	if !strings.Contains(message, "Status: 502") || !strings.Contains(message, "no body returned") {
		t.Fatalf("SubmitMagnet() error = %q, want Status: 502 with no body returned", message)
	}
}

func TestSubmitMagnetCachePreflight(t *testing.T) {
	for _, tc := range []struct {
		name             string
		checkStatus      int
		checkBody        string
		downloadUncached bool
		wantCreates      int
		wantChecks       int
		wantError        string
	}{
		{name: "uncached", checkBody: `{"success":true,"data":{}}`, wantError: "DOWNLOAD_NOT_CACHED", wantChecks: 1},
		{name: "uncached null", checkBody: `{"success":true,"data":null}`, wantError: "DOWNLOAD_NOT_CACHED", wantChecks: 1},
		{name: "cached", checkBody: `{"success":true,"data":{"aabbcc":{"hash":"AABBCC","size":100}}}`, wantChecks: 1, wantCreates: 1},
		{name: "cache check unavailable", checkStatus: http.StatusBadGateway, wantChecks: 1, wantError: "cache check failed"},
		{name: "invalid cache result", checkBody: `{"success":false,"data":null}`, wantChecks: 1, wantError: "cache check failed"},
		{name: "explicit uncached download", downloadUncached: true, wantCreates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checks, creates := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/torrents/checkcached":
					checks++
					if got := r.URL.Query().Get("hash"); got != "AABBCC" {
						t.Errorf("checked hash = %q", got)
					}
					if tc.checkStatus != 0 {
						w.WriteHeader(tc.checkStatus)
					}
					_, _ = fmt.Fprint(w, tc.checkBody)
				case "/api/torrents/createtorrent":
					creates++
					if got := r.FormValue("add_only_if_cached"); got != "true" && !tc.downloadUncached {
						t.Errorf("add_only_if_cached = %q", got)
					}
					_, _ = fmt.Fprint(w, `{"success":true,"data":{"torrent_id":41,"hash":"AABBCC"}}`)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer server.Close()
			torrent := &debridTypes.Torrent{InfoHash: "AABBCC", Magnet: &utils.Magnet{Link: "magnet:?xt=urn:btih:AABBCC"}, DownloadUncached: tc.downloadUncached}
			got, err := testTorbox(server.URL).SubmitMagnet(torrent)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error = %v, want %q", err, tc.wantError)
				}
			} else if err != nil || got == nil || got.Id != "41" {
				t.Fatalf("SubmitMagnet() = %#v, %v", got, err)
			}
			if checks != tc.wantChecks || creates != tc.wantCreates {
				t.Fatalf("checks/creates = %d/%d, want %d/%d", checks, creates, tc.wantChecks, tc.wantCreates)
			}
		})
	}
}

func TestGetTorboxStatusKeepsIncompleteDownloadsRetryable(t *testing.T) {
	tb := &Torbox{}
	if got := tb.getTorboxStatus("incomplete", false); got != debridTypes.TorrentStatusDownloading {
		t.Fatalf("getTorboxStatus(incomplete) = %q, want downloading", got)
	}
}

func TestUpdateTorrentTreatsPresentStalledDownloadAsCompleted(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"id":17,"name":"Reacher.S02.mkv","size":100,"progress":1,"download_state":"stalled (no seeds)","download_finished":false,"download_present":true,"created_at":"2026-01-02T03:04:05Z","hash":"5004BCC4598206C7C7293353936708BC8288C034","files":[{"id":1,"name":"Reacher.S02.mkv","absolute_path":"Reacher.S02.mkv","size":100}]}}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{Id: "17"}
	if err := testTorbox(server.URL).UpdateTorrent(torrent); err != nil {
		t.Fatalf("UpdateTorrent() error = %v", err)
	}
	if torrent.Status != debridTypes.TorrentStatusDownloaded {
		t.Fatalf("Status = %q, want downloaded", torrent.Status)
	}
	file := torrent.Files["Reacher.S02.mkv"]
	if file.Link != "torbox://17/1" {
		t.Fatalf("file link = %q, want torbox://17/1", file.Link)
	}
}

func TestUpdateTorrentDoesNotCompletePartialPresentDownload(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"id":18,"name":"Partial.Release.mkv","size":100,"progress":0.5,"download_state":"downloading","download_finished":false,"download_present":true,"created_at":"2026-01-02T03:04:05Z","hash":"DDEEFF","files":[{"id":1,"name":"Partial.Release.mkv","absolute_path":"Partial.Release.mkv","size":100}]}}`)
	}))
	t.Cleanup(server.Close)

	torrent := &debridTypes.Torrent{Id: "18"}
	if err := testTorbox(server.URL).UpdateTorrent(torrent); err != nil {
		t.Fatalf("UpdateTorrent() error = %v", err)
	}
	if torrent.Status != debridTypes.TorrentStatusDownloading {
		t.Fatalf("Status = %q, want downloading", torrent.Status)
	}
	if link := torrent.Files["Partial.Release.mkv"].Link; link != "" {
		t.Fatalf("file link = %q, want empty until progress reaches 100%%", link)
	}
}

func TestCheckStatusPreservesTorboxFailureContext(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"id":23,"name":"Broken.Release.mkv","size":100,"progress":1,"download_state":"failed (processing)","download_finished":false,"download_present":false,"tracker_message":"storage unavailable","created_at":"2026-01-02T03:04:05Z","hash":"AABBCC","files":[]}}`)
	}))
	t.Cleanup(server.Close)

	torrent, err := testTorbox(server.URL).CheckStatus(&debridTypes.Torrent{Id: "23"})
	if err == nil {
		t.Fatal("CheckStatus() error = nil, want provider failure")
	}
	for _, want := range []string{"failed (processing)", "storage unavailable", "id 23", "hash AABBCC"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("CheckStatus() error = %q, want %q", err, want)
		}
	}
	if torrent == nil || torrent.InfoHash != "AABBCC" {
		t.Fatalf("CheckStatus() torrent = %#v, want returned provider hash", torrent)
	}
}

func TestGetTorrentsBypassesTorboxCache(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var (
		mu      sync.Mutex
		offsets []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("bypass_cache"); got != "true" {
			t.Errorf("bypass_cache = %q, want true", got)
		}

		offset := r.URL.Query().Get("offset")
		mu.Lock()
		offsets = append(offsets, offset)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if offset == "0" {
			_, _ = fmt.Fprint(w, `{"success":true,"data":[{"id":17,"name":"Release.mkv","size":100,"progress":1,"download_state":"completed","download_finished":true,"created_at":"2026-01-02T03:04:05Z","hash":"ABC","files":[{"id":1,"name":"Release.mkv","absolute_path":"Release.mkv","size":100}]}]}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"success":true,"data":[]}`)
	}))
	t.Cleanup(server.Close)

	tb := testTorbox(server.URL)
	torrents, err := tb.GetTorrents()
	if err != nil {
		t.Fatalf("GetTorrents() error = %v", err)
	}
	if len(torrents) != 1 || torrents[0].Id != "17" {
		t.Fatalf("GetTorrents() = %#v, want torrent 17", torrents)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(offsets, []string{"0", "1"}) {
		t.Fatalf("offsets = %v, want [0 1]", offsets)
	}
}

func TestGetTorrentsReturnsPaginationErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") == "0" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"success":true,"data":[{"id":17,"name":"Release.mkv","created_at":"2026-01-02T03:04:05Z"}]}`)
			return
		}
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	tb := testTorbox(server.URL)
	torrents, err := tb.GetTorrents()
	if err == nil {
		t.Fatal("GetTorrents() error = nil, want pagination error")
	}
	if torrents != nil {
		t.Fatalf("GetTorrents() torrents = %#v, want nil after pagination error", torrents)
	}
	if got := err.Error(); !strings.Contains(got, "get TorBox torrents at offset 1:") {
		t.Fatalf("GetTorrents() error = %q, want offset context", got)
	}
}

func TestDeleteTorrentPostsControlJSON(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	var (
		method      string
		gotPath     string
		contentType string
		rawBody     []byte
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		gotPath = r.URL.Path
		contentType = r.Header.Get("Content-Type")
		rawBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"success":true,"error":null,"detail":"Torrent deleted successfully.","data":null}`)
	}))
	t.Cleanup(server.Close)

	if err := testTorbox(server.URL).DeleteTorrent("41"); err != nil {
		t.Fatalf("DeleteTorrent() error = %v", err)
	}
	if method != http.MethodPost {
		t.Fatalf("method = %q, want POST", method)
	}
	if gotPath != "/api/torrents/controltorrent" {
		t.Fatalf("path = %q, want /api/torrents/controltorrent", gotPath)
	}
	if contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}

	var payload map[string]any
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		t.Fatalf("body %q: %v", rawBody, err)
	}
	id, ok := payload["torrent_id"].(float64)
	if !ok || id != 41 {
		t.Fatalf("torrent_id = %#v, want 41", payload["torrent_id"])
	}
	if payload["operation"] != "delete" {
		t.Fatalf("operation = %#v, want delete", payload["operation"])
	}
	if payload["all"] != false {
		t.Fatalf("all = %#v, want false", payload["all"])
	}
}

func TestDeleteTorrentRejectsNonNumericID(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(server.Close)

	err := testTorbox(server.URL).DeleteTorrent("not-a-number")
	if err == nil || !strings.Contains(err.Error(), "invalid torrent id") {
		t.Fatalf("DeleteTorrent() error = %v, want invalid torrent id", err)
	}
	if called {
		t.Fatal("DeleteTorrent() sent a request for a non-numeric id")
	}
}

func testTorbox(host string) *Torbox {
	return &Torbox{
		Host:   host,
		client: request.New(request.WithMaxRetries(0)),
		logger: zerolog.Nop(),
		config: config.Debrid{Name: "torbox"},
	}
}
