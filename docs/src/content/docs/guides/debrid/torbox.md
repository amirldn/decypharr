---
title: Torbox Setup
description: Configure Torbox provider.
---

Torbox is a supported Debrid provider.

## Configuration

```json
{
  "debrids": [
    {
      "provider": "torbox",
      "name": "Torbox",
      "api_key": "YOUR_API_KEY"
    }
  ]
}
```

Get your API key from the Torbox dashboard.

All configuration options from [Real Debrid](./real-debrid/) apply (rate limits, workers, proxy, etc.).

See [Configuration Reference](../configuration/#debrid-providers) for full options.

## Cache-only torrent submissions

When uncached downloads are disabled, Decypharr checks the magnet's info hash
with TorBox's `GET /api/torrents/checkcached` before creating a torrent. An
uncached result returns `DOWNLOAD_NOT_CACHED` to the requesting Arr without a
`createtorrent` call. A cache-check failure returns a separate error, so it
cannot be mistaken for a confirmed miss. A confirmed hit still uses
`add_only_if_cached=true` when creating the torrent.

Explicitly enabled uncached downloads bypass this preflight. TorBox caches
cache-check results for up to an hour, so a newly cached torrent may take time
to become eligible. Availability may also change between the check and create.

See TorBox's [cache-check API](https://www.postman.com/torbox/torbox-api/documentation/b6l9hbv/main-api) and
[API rate limits](https://support.torbox.app/en/articles/13726368-api-rate-limits).

## Rate-limit recovery

The TorBox gate stops automatic retries as soon as a provider request returns
HTTP 429. A valid `Retry-After` is honored in full, even when it exceeds the
configured retry backoff. API calls fail fast; streaming reads can wait for a
short remaining cooldown. Calls through reconstructed TorBox clients share the
same gate while Decypharr is running.

The gate writes a journal under the Decypharr config directory at
`torbox-gate/<SHA-256 of API key>.json`. Keep this directory on persistent
storage with the config. A `pending` request, a missing journal in an existing
gate directory, an unreadable journal, or a 429 without a usable `Retry-After`
requires operator recovery. A restart after a 429 also requires operator
recovery: the process cannot carry its
monotonic timer across a restart and will not infer permission to retry from
the wall clock.

To recover, stop Decypharr, verify the TorBox ban has ended, and back up the
gate journal. Set its state to `{"status":"idle"}` while Decypharr is stopped,
then restart and confirm TorBox requests resume without another 429. Never
clear the journal while the process is running. If the journal cannot be
written and synced, Decypharr refuses new TorBox wire requests.
