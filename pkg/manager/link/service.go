package link

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/customerror"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"golang.org/x/sync/singleflight"
)

const (
	MaxReinsertionAttempt = 3
	// maxValidatedEntries caps the validated-link memo map (see GetLink).
	maxValidatedEntries = 8192
	// slowHostCooldown is how long a CDN host that served a degraded stream
	// is deprioritized when fetching fresh links. Long enough to outlast a
	// transient bad node, short enough to forgive a recovered one.
	slowHostCooldown = 30 * time.Minute
	// maxCooldownHosts caps the slow-host map; resetting merely costs
	// re-learning which hosts are slow.
	maxCooldownHosts = 256
	// maxCooldownSkips bounds how many freshly dealt links are discarded for
	// pointing at cooling hosts before falling back to the last one dealt.
	maxCooldownSkips = 3
)

var (
	emptyDownloadLink = types.DownloadLink{}
)

// EntryRefresher is a function that refreshes an entry by infohash
type EntryRefresher func(infohash string) (*storage.Entry, error)
type EntryRepairer func(ctx context.Context, entry *storage.Entry) error
type EntrySaver func(entry *storage.Entry) error

// Service handles download link fetching and validation.
// It uses the account-level cache for storing links and only tracks validation state.
type Service struct {
	validated      *xsync.Map[string, error]
	cooldowns      *xsync.Map[string, time.Time]
	singleflight   singleflight.Group
	clients        *xsync.Map[string, debrid.Client]
	entryRefresher EntryRefresher
	repairer       EntryRepairer
	entrySaver     EntrySaver
	httpClient     *http.Client
	retries        int
	logger         zerolog.Logger
}

// New creates a new LinkService
func New(
	clients *xsync.Map[string, debrid.Client],
	entryRefresher EntryRefresher,
	entryReinsert EntryRepairer,
	entrySaver EntrySaver,
	httpClient *http.Client,
	retries int,
	logger zerolog.Logger,
) *Service {
	return &Service{
		validated:      xsync.NewMap[string, error](),
		cooldowns:      xsync.NewMap[string, time.Time](),
		clients:        clients,
		entryRefresher: entryRefresher,
		repairer:       entryReinsert,
		entrySaver:     entrySaver,
		httpClient:     httpClient,
		retries:        retries,
		logger:         logger,
	}
}

// GetLink fetches and validates a download link for a file in an entry.
// Links are cached at the account level; this service only tracks validation state.
func (s *Service) GetLink(ctx context.Context, entry *storage.Entry, filename string) (types.DownloadLink, error) {
	// Use singleflight to deduplicate concurrent requests for the same file
	key := entry.InfoHash + ":" + filename
	v, err, _ := s.singleflight.Do(key, func() (any, error) {
		return s.fetchAndValidate(ctx, entry, filename, 0)
	})

	if err != nil {
		return emptyDownloadLink, err
	}

	return v.(types.DownloadLink), nil
}

// Refresh invalidates a link that failed mid-stream and fetches a replacement.
// It shares GetLink's singleflight key, so a concurrent GetLink may win the
// race and hand back the stale link once more; callers operate on bounded
// retry budgets, so the follow-up attempt lands after the refresh completes.
func (s *Service) Refresh(ctx context.Context, entry *storage.Entry, bad types.DownloadLink) (types.DownloadLink, error) {
	if bad.Filename == "" {
		return emptyDownloadLink, NewPermanentError(ErrEmptyLink, "empty_link")
	}
	key := entry.InfoHash + ":" + bad.Filename
	v, err, _ := s.singleflight.Do(key, func() (any, error) {
		return s.invalidateAndRefetch(ctx, entry, bad, 0)
	})
	if err != nil {
		return emptyDownloadLink, err
	}
	return v.(types.DownloadLink), nil
}

func (s *Service) getClient(provider string) (debrid.Client, error) {
	c, ok := s.clients.Load(provider)
	if !ok {
		return nil, fmt.Errorf("client for provider %s not found", provider)
	}
	return c, nil
}

// fetchAndValidate fetches a download link and validates it.
// attempt tracks how many re-insertion cycles we've already paid for during
// this GetLink call so we can bail out instead of looping forever when the
// underlying file never resolves (see fetchLink/handleBadLink).
func (s *Service) fetchAndValidate(ctx context.Context, entry *storage.Entry, filename string, attempt int) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return emptyDownloadLink, err
	}
	link, err := s.fetchLink(ctx, entry, filename, attempt)
	if err != nil {
		return s.handleBadLink(ctx, err, entry, link, attempt)
	}

	// A link we already know is expired cannot be validated back to life: the
	// HEAD below would burn the whole retry ladder before landing in
	// invalidateAndRefetch anyway. Refetch first, then validate the fresh link
	// once through the normal path.
	if link.Expired() && link.Debrid != "" {
		fresh, refetchErr := s.invalidateAndRefetch(ctx, entry, link, attempt)
		if refetchErr != nil {
			return fresh, refetchErr
		}
		link = fresh
	}

	// Is link already validated
	// Check if we've already validated this link
	if validationErr, exists := s.validated.Load(link.DownloadLink); exists {
		if validationErr == nil {
			// Re-fetch if the cached CDN URL has passed its declared expiry.
			// Without this check a 3-hour TorBox CDN URL would be served from
			// s.validated forever — bypassing the HEAD validation that would
			// otherwise catch the expired URL.
			if !link.ExpiresAt.IsZero() && time.Now().After(link.ExpiresAt) {
				return s.invalidateAndRefetch(ctx, entry, link, attempt)
			}
			return link, nil // Already validated successfully
		}
		// Previous validation failed - check if we should retry
		if linkErr := GetLinkError(validationErr); linkErr != nil {
			if linkErr.ShouldRefetch() {
				// Invalidate and refetch
				return s.invalidateAndRefetch(ctx, entry, link, attempt)
			}
		}
		return emptyDownloadLink, validationErr
	}

	// Validate the link
	validationErr := s.validateLink(ctx, &link)
	// Backpressure is transient provider state, never a cached link failure.
	if e := request.BackpressureError(validationErr); e != nil {
		return emptyDownloadLink, e
	}

	if validationErr != nil {
		// Handle link error categories
		if linkErr := GetLinkError(validationErr); linkErr != nil {
			if linkErr.ShouldDisableAccount() {
				if err := s.disableLinkAccount(link, linkErr); err != nil {
					s.logger.Error().
						Err(err).
						Str("debrid", link.Debrid).
						Str("token", utils.Mask(link.Token)).
						Str("reason", linkErr.Code).
						Msg("Failed to disable account after link error")
				} else {
					// This will use the next available account and fetch a new link, so we need to refetch and revalidate.
					// Account swap doesn't consume a re-insertion attempt.
					return s.fetchAndValidate(ctx, entry, filename, attempt)
				}
			} else if linkErr.ShouldRefetch() {
				// The link itself is stale or rejected; a fresh one is needed.
				return s.invalidateAndRefetch(ctx, entry, link, attempt)
			} else if linkErr.ShouldRetry() {
				// Transient provider/CDN state (5xx, wire blip, unknown code):
				// the link itself is not known to be bad, so do not delete it
				// or spend another requestdl on a refetch — invalidating the
				// cached link per attempt is upstream #381's poll-driven API
				// flood. Return the error without memoising it: the next
				// GetLink revalidates the same link, so a wobble that has
				// cleared recovers on the next attempt.
				return emptyDownloadLink, validationErr
			}
		}
	}

	// Store validation result
	// Keys are full download URLs and links rotate on refresh/expiry, so cap
	// the map; resetting merely costs a re-validation per link.
	if s.validated.Size() > maxValidatedEntries {
		s.validated.Clear()
	}
	s.validated.Store(link.DownloadLink, validationErr)

	if validationErr == nil {
		return link, nil
	}
	return emptyDownloadLink, validationErr
}

func (s *Service) handleBadLink(ctx context.Context, err error, entry *storage.Entry, dl types.DownloadLink, attempt int) (types.DownloadLink, error) {
	if errors.Is(err, customerror.HosterUnavailableError) {
		if entry.Bad {
			return emptyDownloadLink, fmt.Errorf("can't repair %s since it's been marked as bad", entry.GetFolder())
		}
		if attempt >= MaxReinsertionAttempt {
			s.markEntryBad(entry, dl.Filename, attempt, "hoster_unavailable")
			return emptyDownloadLink, fmt.Errorf("entry %s file %s still unresolvable after %d re-insertion attempts", entry.GetFolder(), dl.Filename, attempt)
		}
		if err := s.repairer(ctx, entry); err != nil {
			return emptyDownloadLink, err
		}

		if entry.Bad {
			// Entry is still bad
			return emptyDownloadLink, fmt.Errorf("entry %s(%s) still bad after repair, un-repairable", entry.GetFolder(), dl.Link)
		}
		// Bypass singleflight re-entry to avoid deadlock
		return s.fetchAndValidate(ctx, entry, dl.Filename, attempt+1)
	}
	// Just return the error
	return dl, err
}

// markEntryBad sets entry.Bad and persists it so subsequent GetLink calls
// for the same entry short-circuit instead of triggering another re-insertion
// cycle. Logged once per call.
func (s *Service) markEntryBad(entry *storage.Entry, filename string, attempt int, reason string) {
	entry.Bad = true
	if s.entrySaver != nil {
		if err := s.entrySaver(entry); err != nil {
			s.logger.Warn().
				Err(err).
				Str("infohash", entry.InfoHash).
				Msg("Failed to persist Bad flag after exhausting re-insertion attempts")
		}
	}
	s.logger.Warn().
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Str("filename", filename).
		Int("attempts", attempt).
		Str("reason", reason).
		Msg("Giving up on entry after repeated failed re-insertions")
}

// fetchLink fetches a download link from the debrid provider (via account cache)
func (s *Service) fetchLink(ctx context.Context, entry *storage.Entry, filename string, attempt int) (types.DownloadLink, error) {
	file, err := entry.GetFile(filename)
	if err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file %s not found in entry %s: %w", filename, entry.Name, err),
			"file_not_found",
		)
	}

	placementFile, err := s.getPlacementFile(entry, filename)
	if err != nil {
		return emptyDownloadLink, err
	}

	if placementFile.Link == "" && placementFile.Id == "" {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file link is missing for %s in entry %s", filename, entry.Name),
			"link_missing",
		)
	}

	client, err := s.getClient(entry.ActiveProvider)
	if err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("debrid client not found: %s", entry.ActiveProvider),
			"client_not_found",
		)
	}

	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("no placement found for debrid %s with infohash %s", entry.ActiveProvider, entry.InfoHash),
			"placement_not_found",
		)
	}

	debridFile := &types.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}

	// TorBox resolves its requestdl placeholder with the caller's playback
	// context. Its plain GetDownloadLink remains free of network calls for repair.
	var downloadLink types.DownloadLink
	if playback, ok := client.(interface {
		GetDownloadLinkForPlayback(context.Context, string, *types.File) (types.DownloadLink, error)
	}); ok {
		downloadLink, err = playback.GetDownloadLinkForPlayback(request.WithClass(ctx, request.ClassPlayback), placement.ID, debridFile)
	} else {
		downloadLink, err = client.GetDownloadLink(ctx, placement.ID, debridFile)
	}
	if err != nil {
		return downloadLink, err
	}

	if downloadLink.Empty() {
		// Let's try to reinsert the entry
		if entry.Bad {
			return emptyDownloadLink, fmt.Errorf("can't repair %s since it's been marked as bad", entry.GetFolder())
		}
		if attempt >= MaxReinsertionAttempt {
			s.markEntryBad(entry, filename, attempt, "empty_link")
			return emptyDownloadLink, fmt.Errorf("entry %s file %s still resolves to an empty link after %d re-insertion attempts", entry.GetFolder(), filename, attempt)
		}
		if err := s.repairer(ctx, entry); err != nil {
			return emptyDownloadLink, err
		}

		if entry.Bad {
			// Entry is still bad
			return emptyDownloadLink, fmt.Errorf("entry %s(%s) still bad after repair, un-repairable", entry.GetFolder(), downloadLink.Link)
		}
		// Bypass singleflight re-entry to avoid deadlock
		return s.fetchAndValidate(ctx, entry, filename, attempt+1)
	}

	return downloadLink, nil
}

// getPlacementFile retrieves the placement file with refresh fallback
func (s *Service) getPlacementFile(entry *storage.Entry, filename string) (*storage.ProviderFile, error) {
	_, ok := entry.Files[filename]
	if !ok {
		return nil, NewPermanentError(
			fmt.Errorf("file %s not found in entry", filename),
			"file_not_found",
		)
	}

	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil {
		return nil, NewPermanentError(
			fmt.Errorf("no placement found for debrid %s with infohash %s", entry.ActiveProvider, entry.InfoHash),
			"placement_not_found",
		)
	}

	placementFile := placement.Files[filename]
	if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
		if s.entryRefresher == nil {
			return nil, NewPermanentError(
				fmt.Errorf("file %s not available and no refresher configured", filename),
				"no_refresher",
			)
		}

		refreshed, err := s.entryRefresher(entry.InfoHash)
		if err != nil {
			return nil, NewRefetchableError(
				fmt.Errorf("failed to refresh entry: %w", err),
				"refresh_failed",
			)
		}

		file := refreshed.Files[filename]
		if file == nil {
			return nil, NewPermanentError(
				fmt.Errorf("file disappeared after refresh"),
				"file_disappeared",
			)
		}

		placement = refreshed.Providers[entry.ActiveProvider]
		if placement == nil {
			return nil, NewPermanentError(
				fmt.Errorf("placement disappeared after refresh for debrid %s", entry.ActiveProvider),
				"placement_disappeared",
			)
		}

		placementFile = placement.Files[filename]
		if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
			return nil, NewPermanentError(
				fmt.Errorf("file %s not available after refresh", filename),
				"file_not_available",
			)
		}

		*entry = *refreshed
	}

	return placementFile, nil
}

// validateLink validates a download link by making a HEAD request
func (s *Service) validateLink(ctx context.Context, link *types.DownloadLink) error {
	if link == nil {
		return NewPermanentError(ErrEmptyLink, "empty_link")
	}
	if link.Empty() {
		return NewPermanentError(fmt.Errorf("download url is empty for %s||%s", link.Filename, link.Link), "empty_link")
	}

	req, err := http.NewRequestWithContext(ctx, "HEAD", link.DownloadLink, nil)
	if err != nil {
		return NewPermanentError(
			fmt.Errorf("failed to create HEAD request: %w", err),
			"request_creation_failed",
		)
	}

	var resp *http.Response
	if provider, ok := s.clients.Load(link.Debrid); ok {
		if p, ok := provider.(request.ThrottleProvider); ok && p.RequestThrottle() != nil {
			resp, err = p.RequestThrottle().Do(s.httpClient, req)
		} else {
			resp, err = s.httpClient.Do(req)
		}
	} else {
		resp, err = s.httpClient.Do(req)
	}
	if err != nil {
		if e := request.BackpressureError(err); e != nil {
			return e
		}
		// Strip the URL credentials (the requestdl token query parameter)
		// before the error is logged anywhere.
		return NewRetryableError(
			fmt.Errorf("HEAD request failed: %w", request.RedactURLError(err)),
			"network_error",
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// A cached link can point at a CDN host that is cooling down after
		// serving a degraded stream; treat it as refetchable so a fresh
		// link — preferably on another host — is dealt instead of
		// re-serving the slow node.
		if s.hostCooling(finalHost(resp)) {
			return NewRefetchableError(
				fmt.Errorf("CDN host %s in slow-stream cooldown", finalHost(resp)),
				"host_cooldown",
			)
		}
		return nil
	}

	errorCode := resp.Header.Get("X-Error")
	if errorCode == "" {
		errorCode = strconv.Itoa(resp.StatusCode)
	}

	return ErrorCodeToLinkError(errorCode)
}

// Resolve follows a download link's requestdl redirect through the provider's
// shared throttle and returns the final CDN URL. Callers that hand a link to an
// out-of-process consumer (for example the /api/browse download redirect) use
// it so the requestdl call is charged to the shared budget instead of leaking
// past Decypharr. It is deliberately provider-agnostic (not TorBox-only):
// validateLink already HEADs the same URL for every provider, so this adds one
// hop only on the /api/browse download route, after validation has succeeded.
func (s *Service) Resolve(ctx context.Context, link types.DownloadLink) (string, error) {
	if link.Empty() {
		return "", NewPermanentError(ErrEmptyLink, "empty_link")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, link.DownloadLink, nil)
	if err != nil {
		return "", NewPermanentError(
			fmt.Errorf("failed to create resolve request: %w", err),
			"request_creation_failed",
		)
	}
	client := s.httpClient
	if provider, ok := s.clients.Load(link.Debrid); ok {
		if p, ok := provider.(request.ThrottleProvider); ok && p.RequestThrottle() != nil {
			resp, err := p.RequestThrottle().Do(client, req)
			return s.finalURL(resp, err)
		}
	}
	resp, err := client.Do(req)
	return s.finalURL(resp, err)
}

// finalURL closes the probe response and returns the last URL in the redirect
// chain. Backpressure passes through as a typed error.
func (s *Service) finalURL(resp *http.Response, err error) (string, error) {
	if err != nil {
		if e := request.BackpressureError(err); e != nil {
			return "", e
		}
		// Strip the requestdl token query parameter before this can be logged.
		return "", NewRetryableError(
			fmt.Errorf("resolve request failed: %w", request.RedactURLError(err)),
			"network_error",
		)
	}
	if resp == nil {
		return "", NewRetryableError(fmt.Errorf("resolve returned no response"), "network_error")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Require a followed 2xx: an un-followed 3xx would otherwise hand back
		// the token-bearing requestdl URL as the "resolved" target.
		errorCode := resp.Header.Get("X-Error")
		if errorCode == "" {
			errorCode = strconv.Itoa(resp.StatusCode)
		}
		return "", ErrorCodeToLinkError(errorCode)
	}
	if resp.Request != nil && resp.Request.URL != nil && resp.Request.URL.String() != "" {
		return resp.Request.URL.String(), nil
	}
	if location := resp.Header.Get("Location"); location != "" {
		return location, nil
	}
	return "", NewRetryableError(fmt.Errorf("resolve returned no download URL"), "network_error")
}

// disableLinkAccount handles errors that require disabling an account
func (s *Service) disableLinkAccount(link types.DownloadLink, linkErr *Error) error {
	client, err := s.getClient(link.Debrid)
	if err != nil {
		return fmt.Errorf("failed to get client for debrid %s: %w", link.Debrid, err)
	}

	accountManager := client.AccountManager()
	account, err := accountManager.GetAccount(link.Token)
	if err != nil {
		return fmt.Errorf("failed to get account for token %s: %w", utils.Mask(link.Token), err)
	}

	if account == nil {
		return fmt.Errorf("account not found for token %s", utils.Mask(link.Token))
	}

	accountManager.Disable(account)

	// Remove all validations for all the links
	s.validated.Clear()
	s.logger.Warn().
		Str("debrid", link.Debrid).
		Str("token", utils.Mask(account.Token)).
		Str("account", utils.Mask(account.Username)).
		Str("reason", linkErr.Code).
		Msg("Disabled account due to error")
	return nil
}

// invalidateAndRefetch removes a link from both validation tracking and account cache
func (s *Service) invalidateAndRefetch(ctx context.Context, entry *storage.Entry, link types.DownloadLink, attempt int) (types.DownloadLink, error) {
	// Remove from validation tracking
	s.validated.Delete(link.DownloadLink)

	// Remove from account cache
	if link.Debrid == "" {
		return emptyDownloadLink, fmt.Errorf("invalid link")
	}

	client, err := s.getClient(link.Debrid)
	if err != nil {
		return emptyDownloadLink, err
	}

	_ = client.DeleteLink(link) // This might fail, doesnt matter

	fresh, err := s.fetchLink(ctx, entry, link.Filename, attempt)
	if err != nil {
		return fresh, err
	}
	// A freshly dealt link can still land on a cooling CDN host; discard a
	// bounded number of them before falling back to the last one dealt.
	// CDNHost reads the link URL, not the final post-redirect URL: correct
	// for pre-resolved providers (TorBox), documented no-op otherwise —
	// see CDNHost.
	for skipped := 0; skipped < maxCooldownSkips && s.hostCooling(CDNHost(fresh)); skipped++ {
		s.logger.Info().
			Str("host", CDNHost(fresh)).
			Int("skipped", skipped+1).
			Msg("Fresh link points at a cooling CDN host; refetching")
		_ = client.DeleteLink(fresh)
		if fresh, err = s.fetchLink(ctx, entry, link.Filename, attempt); err != nil {
			return fresh, err
		}
	}
	return fresh, nil
}

// Clear removes all validation tracking entries
func (s *Service) Clear() {
	s.validated.Clear()
}

// NoteSlowHost records that host served a degraded stream and deprioritizes
// it for slowHostCooldown when fresh links are dealt. Called from the stream
// recovery path after the slow-stream watchdog trips.
func (s *Service) NoteSlowHost(host string, bps float64) {
	if host == "" {
		return
	}
	if s.cooldowns.Size() >= maxCooldownHosts {
		s.sweepCooldowns()
		if s.cooldowns.Size() >= maxCooldownHosts {
			return // fail soft: drop the entry rather than grow unbounded
		}
	}
	s.cooldowns.Store(host, time.Now().Add(slowHostCooldown))
	s.logger.Warn().
		Str("host", host).
		Float64("kbps", bps/1024).
		Dur("cooldown", slowHostCooldown).
		Msg("CDN host served a degraded stream; cooling down")
}

// sweepCooldowns drops expired entries from the slow-host map.
func (s *Service) sweepCooldowns() {
	now := time.Now()
	s.cooldowns.Range(func(host string, exp time.Time) bool {
		if now.After(exp) {
			s.cooldowns.Delete(host)
		}
		return true
	})
}

// hostCooling reports whether host is inside its slow-stream cooldown.
// Expired entries are reaped lazily on read.
func (s *Service) hostCooling(host string) bool {
	if host == "" {
		return false
	}
	exp, ok := s.cooldowns.Load(host)
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		s.cooldowns.Delete(host)
		return false
	}
	return true
}

// CDNHost extracts the host serving the bytes for a download link.
//
// ASSUMPTION: for providers whose playback links are pre-resolved CDN URLs
// (TorBox: GetDownloadLinkForPlayback resolves requestdl before caching) this
// is the CDN node itself, and it agrees with the final-URL host the stream
// watchdog and validateLink measure. For providers whose links are
// redirectors, this is the redirector host: a cooldown recorded against the
// final CDN host then never matches here, so the skip filter degrades to a
// no-op rather than misfiring. If a redirector-style provider ever needs the
// filter, resolve the link (Service.Resolve) before comparing.
func CDNHost(dl types.DownloadLink) string {
	u, err := url.Parse(dl.DownloadLink)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

// finalHost returns the host of the last URL in a response's redirect chain.
func finalHost(resp *http.Response) string {
	if resp != nil && resp.Request != nil && resp.Request.URL != nil {
		return resp.Request.URL.Host
	}
	return ""
}
