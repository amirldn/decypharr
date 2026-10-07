# KGBos changes on upstream v2.6

## Sources

- Upstream v2.6: `3a0e0e23de153c70b2d441e14a7828b29f6e8391`.
- KGBos master: `71ece258b2a2728cbd06014a083d194d6b03a00f`, September 26, 2026: https://github.com/KGBos/decypharr/tree/71ece258b2a2728cbd06014a083d194d6b03a00f
- Amir's deployed source before integration: `d016aba4cd117ec297e2e7e286d538ee6520199e`.

The 33 KGBos downstream commits were consolidated and rebased onto v2.6. Their original history remains in KGBos's fork; this integration retains their implementation and adapts its interfaces to the current upstream APIs.

## Included behavior

TorBox backpressure, persistent cooldown state, shared requestdl budgets and priority lanes, resolved CDN link caching, cached-only preflight, negative caching, submission mediation, broader retryable error classifications, slow-stream failover, completion reconciliation, bounded cache warming, and DFS mount lifecycle protection.

The v2.6 provider context/cancellation and availability-result contracts, shared same-key rate limiter, authentication, durable repair jobs, resumable downloads and shutdown ordering are preserved. Browser JavaScript was rebuilt from the integrated source.

## Amir's patches

- HTTP 400 refetch: already included by KGBos (borrowed from Amir's upstream PR #402), so the local duplicate is not applied.
- VFS directory refresh between symlink retries: reapplied, including disabling repeated refresh attempts after the first failure.
- Category-less qBittorrent login: reapplied to the v2.6 authentication flow; configured Arr host + API key must match, and invalid credentials remain rejected.

## Defaults to review before deployment

The inherited TorBox requestdl budget defaults to **12 requests/minute**; this controls new download-link resolution, not the byte rate of an already resolved CDN stream. Submission mediation is enabled by default, with separate cached and uncached valves (the inherited uncached default is **45/hour**). Existing `skip_pre_cache: true` continues to disable cache warming. The explicit debrid-level `download_uncached` setting now persists false as well as true.

These defaults are from the KGBos implementation, not a change to the deployed server configuration. Publishing this source does not update the running container.

## Integration regression coverage

Tests preserve upstream error-body/cancellation behavior, dedicated submission-client routing, TorBox response compatibility, the availability API's input spelling and explicit negative results, live config snapshots for cache warming, interrupted-download shutdown behavior, and category-less login acceptance/rejection. The upstream and KGBos test suites are retained.
