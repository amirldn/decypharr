package qbit

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/manager"
)

// Sonarr/Radarr POST /api/v2/auth/login with NO category in the URL, sending
// host-as-username and the arr API key as the password. With UseAuth=true this
// must not 401, or GetItems() fails and the whole queue reads
// "downloadClientUnavailable".
func TestLoginWithoutCategoryUnderUseAuth(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.UseAuth = true
	cfg.Arrs = []config.Arr{{Name: "sonarr", Host: "http://sonarr:8989", Token: "f3b7417fa456471ba7103c35fcac3353"}}

	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop() })

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v2/auth/login",
		strings.NewReader("username=http://sonarr:8989&password=f3b7417fa456471ba7103c35fcac3353"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	(&QBit{manager: mgr}).handleLogin(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("login status = %d, want 200. body=%q", recorder.Code, recorder.Body.String())
	}
}

// A bad key must still be rejected - the fix must not become an auth bypass.
func TestLoginWithoutCategoryRejectsWrongToken(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.UseAuth = true
	cfg.Arrs = []config.Arr{{Name: "sonarr", Host: "http://sonarr:8989", Token: "f3b7417fa456471ba7103c35fcac3353"}}

	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop() })

	for _, tc := range []struct{ name, body string }{
		{"wrong token", "username=http://sonarr:8989&password=wrongkey"},
		{"unknown host", "username=http://evil:8989&password=f3b7417fa456471ba7103c35fcac3353"},
		{"empty password", "username=http://sonarr:8989&password="},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/api/v2/auth/login", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			(&QBit{manager: mgr}).handleLogin(recorder, request)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", recorder.Code)
			}
		})
	}
}
