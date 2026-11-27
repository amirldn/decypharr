# Decypharr

## About this fork (amirldn/decypharr)

This is a personal build of [sirrobot01/decypharr](https://github.com/sirrobot01/decypharr) for one Saltbox host:
**TorBox only**, an **rclone** mount (`vfs_cache_mode full`) behind a Gluetun VPN, Plex, and six Arr instances
(HD, 4K and anime Sonarr/Radarr) using decypharr as their qBittorrent client. `main` is exactly what that host
runs. **Everything after this section is upstream's README.**

- **Base:** upstream **v2.7.2** (`d6f8df0`), plus five fixes picked early from upstream `beta`.
- **Deployed image:** `amirldn/decypharr:2.7.2-custom-<sha>` (built locally, not published to a registry).
- **Detailed records:** [`docs/operations/v2.7.2-rebase.md`](docs/operations/v2.7.2-rebase.md) (rebase and picks),
  [`docs/operations/kgbos-v26-integration.md`](docs/operations/kgbos-v26-integration.md) (KGBos work),
  [`docs/operations/mount-lifecycle.md`](docs/operations/mount-lifecycle.md) (mount start/stop).

Most changes target three things: **TorBox's limits** (every wasted `/requestdl` call or re-download costs
rate limit and daily bandwidth), **Plex start-up latency** on a debrid-backed mount, and **imports that stall
or get thrown away** between decypharr and the Arrs. Each item below gives the problem, the change, and the
measured effect where there is one.

### 1. TorBox reliability (from [KGBos/decypharr](https://github.com/KGBos/decypharr), through `71ece25`)

**Problem:** under load TorBox answers 429, sometimes with no retry deadline. Upstream retried too fast,
spent its request budget on background work while a viewer waited, and a burst of season-pack adds or cache
warms tripped further 429s.

**Change:**
- Backpressure with cooldown state that **survives restarts**, and recovery from 429s that carry no deadline
  (exponential backoff) and from transient wire errors.
- One shared `/requestdl` budget (`requestdl_budget`) with **priority lanes**: playback before probe before
  background.
- Resolved CDN links are cached; cached-only preflight; deterministic rejections are cached (negative cache), so
  the same bad release isn't re-submitted.
- One global submission mediator, so concurrent adds of the same hash become a single TorBox call.
- Slow-stream detection that swaps to another CDN link.
- Capped cache-warm concurrency: packs of 4+ files warm one at a time, and one gate stops packs overlapping.

**Kept over upstream:** links resolve **lazily** through the budgeted resolver. Upstream's `8162e05` added a
second, unbudgeted `requestdl` per link, and its `beta` goes further (`0210615`, `0cbdba9`: an eager `requestdl`
per link, no budget). With both, every link would cost two calls. `TestFetchDownloadLinkDefersCDNResolution`
guards this.

**Improvement:** playback no longer queues behind background traffic, and pack grabs and warms stay under
TorBox's rate limit instead of tripping 429 cooldowns that stall everything.

### 2. Edge cache: a persistent head/tail store (`pkg/edgecache`)

**Problem:** the rclone VFS cache is a size-capped LRU that evicts **whole files** by last access. On a full
disk it kept about a day of history, so anything older started cold. One measured start took ~10.5s, ~8s of it
the Plex transcoder probing the file over TorBox (start, end-of-file index, back again; ~1–1.5s first byte each
through the VPN).

**Change:** the first 16 MB and last 4 MB of every media file are kept on local disk (`<key>.head` / `.tail`,
key = sha256 of infohash + filename + size). WebDAV serves those bytes from disk without opening a TorBox
stream, and only the rest of a range goes to TorBox. Fills happen:
- at import, ahead of the backfill;
- through a paced one-off backfill of the whole library (~13s and ~20 MB of TorBox bandwidth per file);
- by capturing playback reads that cover a missing head or tail.

Fills use the **background** lane and pause while anyone is streaming. A daily prune removes files no longer in
the library and never runs against an empty listing. `request.PlaybackUnlessSet` lets stream openers default to
the playback lane while respecting a background lane the caller set.

**Config:** `edge_cache` (`enabled`, `dir`, `head_size`, `tail_size`, `backfill`, `backfill_delay`). Off by
default. About 56 GB for ~2,900 files.

**Improvement:** start-up reads through the mount take **0.04s with head/tail stored, against 3.1–9.0s cold**,
whenever the title was last played. Stored bytes matched an independent rclone copy byte for byte.

### 3. Faster imports

**Report complete, then warm.** Pre-cache warm (first and last 2 MB of up to 5 files) used to run *before*
the download was reported complete, so the Arr couldn't start importing until it finished: 5–11s per film,
37–47s per pack in production logs. Completion is now reported first and the warm runs in the background; the
Arr's next poll overlaps it and the mount cache fetches shared bytes once.

**Bounded add requests.** A degraded TorBox cache check held qBittorrent add requests open until the Arr's 100s
HTTP timeout, once per release; one film took 23 minutes. Adds now give up after **30s** with a retryable
`504 provider_timeout`. Healthy adds take ~1s (p99 7s over 874 adds).

### 4. Stop throwing away good downloads

**15-minute grace before blocklisting import problems.** Queue cleanup ran every 10s and blocklisted a download
the first time its queue item showed an import problem. During a torrent refresh decypharr briefly answered 404
for fresh season-pack files; Sonarr's import check failed for ~20s, and good packs were blocklisted and
re-searched before Sonarr's own retry (about once a minute) could import them. **All 12 failed downloads in the
preceding 11 days were season packs lost this way.** Import problems must now persist for 15 minutes, tracked
per instance and download ID, before blocklist actions run. Downloads the client reports as failed are still
acted on at once.

**Keep partly imported downloads.** A pack covering only part of a season (e.g. half-season anime packs)
reports "One or more episodes expected in this release were not imported" even after its files import. The
`episodes_missing` rule then removed it with `removeFromClient=true`, deleting the files the imported episodes
link to. Those removals now keep the download in decypharr; the release is still blocklisted.

**Don't delete fresh entries that TorBox hasn't listed yet.** The 5-minute refresh deleted any entry missing
from TorBox's torrent list, but that list can lag a new add. A pack added at 01:43:22 was missing from the
01:47:00 refresh, deleted, and re-added at 01:52:00; for those five minutes its files answered 404 and Sonarr's
import failed. This was the cause of 6 of 8 refresh-aligned 404 bursts on fresh season packs. Entries created
or placed on a provider within the last **30 minutes** are now kept (`refreshMissingGrace`).

**Diagnostics:** every WebDAV 404 branch logs which lookup failed (rate limited per path), and each torrent
refresh logs new/updated/deleted counts, timing and every entry it deletes. That logging is what traced the 404
bursts to the refresh above.

### 5. Clean rclone shutdown

**Problem:** every stop left the FUSE mount behind. rclone ran on the application's signal context, so SIGTERM
killed it (SIGKILL) before `Stop()` ran. `Stop()` then cancelled before unmounting, so the RC unmount hit a dead
server and the `fusermount` fallback couldn't start. It still logged success. The restart script's own
`umount -l` hid this.

**Change:** rclone is bound to the manager's own context and stopped with SIGINT (killed after 5s). `Stop()`
unmounts first, under a fresh 4s timeout, then cancels and waits for a single reaper. "Successfully unmounted"
is logged only on success. `TestRcloneOutlivesCallerContextUntilStop` covers it.

**Improvement:** stops log "Unmounting via RC … Successfully unmounted" and leave no stale mount behind, so a
restart can't bring decypharr up on a dead FUSE handle. Upstream later merged the same fix as `1a6a8a2`.

### 6. Smaller fixes

- **VFS refresh between symlink retries:** an Arr symlink to a just-added file is retried after refreshing
  rclone's directory cache, instead of retrying against a stale listing.
- **Category-less qBittorrent login:** the Arr's own URL is the username and its API key the password, so one
  decypharr serves all six Arrs without a category per login. Now upstream's `MatchCredentials`, which
  compares the host exactly (no trailing `/`); the fork's login regression tests are kept.
- **TorBox status handling:** the `queued` state counts as downloading, and the status list is case-insensitive.
  A `createtorrent` reply naming a different hash than the one submitted is rejected.

### 7. Picked early from upstream `beta` (before its next release)

Chosen by reviewing all 46 `beta` commits past v2.7.2:

| Upstream | Problem it fixes here |
|---|---|
| `7ab6565` (#493) | Re-adding a hash deleted inside the dedup window returned "added" but queued nothing. It is now a retryable 409. |
| `ee6511f` (#496) | A torrent whose queue entry failed was never processed again, and the Arr's re-grab was swallowed as a duplicate until restart: a direct cause of stalled imports. It is now submitted again. |
| `c3fa307` | A 429 or network error on a status check failed the entry. It now retries on the next tick. The TorBox copy is deleted only on TorBox's own verdict (failed, not cached, blocked), never on a request error. |
| `b564fcb` (#485) | Bulk Arr actions sending `a\|b` hash lists matched nothing. |
| `4222c0d` (#481) | WebDAV `getlastmodified` is now an RFC-compliant HTTP date. |

`c3fa307` was ported by hand onto the KGBos code: TorBox failures keep the fork's detailed state/reason text
and also wrap `TorrentFailedError`; a 404 or empty `mylist` reply is `TorrentNotFoundError`.

**Deliberately not taken:** `0210615` + `0cbdba9` (eager, unbudgeted `requestdl` per link; would undo item 1's
lazy budgeted resolver) and `6fbd505` / `a0a11ed` (upstream's own take on the KGBos add and throttle work; to be
reconciled at the next release).

### Where upstream already had our fix

On rebasing onto v2.7.2, upstream code replaced fork code wherever upstream had shipped the same fix, and the
fork's tests were kept against upstream's version: expired-link refetch (#439), 400/403/410 link refetch (#434),
array `createtorrent` replies (#417), category-less Arr login (`66c2cb5`).

### Branches

- `main`: the deployed build.
- `feat/v2.7.2-upstream-picks`, `rebase/v2.7.2`: the current line (v2.7.2 + fork, with and without the `beta`
  picks).
- `integrate/kgbos-v2.6`, `feat/edge-cache`, `feat/async-warm-add-timeout`: the previous, v2.6-based line.
- `archive/main-2026-10-11`: the pre-v2.6 custom build.

### Building and testing

```sh
docker build --build-arg VERSION=2.7.2-custom-<sha> --build-arg CHANNEL=stable -t amirldn/decypharr:2.7.2-custom-<sha> .
```

Run `npm ci && node scripts/minify-js.js` after changing `pkg/server/assets/js`. Tests: `go test ./...` (40
packages pass; needs `libfuse-dev`).

---


![ui](docs/src/assets/images/index.png)

**Decypharr** is a **Media Gateway** for Debrid services and Usenet written in Go.

## What is Decypharr?

Decypharr provides a unified interface for Sonarr, Radarr, and other *Arr applications to access Debrid providers and
Usenet streaming.

## Features

- Mock Qbittorent and Sabnzbd API that supports the Arrs (Sonarr, Radarr, Lidarr etc)
- Multiple Debrid and usenet providers support with a single interface
- Direct Usenet streaming via NNTP (no separate download client required)
- Read-only NFSv4 and SMB servers for the same libraries, including custom virtual folders

## Supported Debrid Providers

- [Real Debrid](https://real-debrid.com)
- [Torbox](https://torbox.app)
- [Debrid Link](https://debrid-link.com)
- [All Debrid](https://alldebrid.com)
- [Premiumize](https://www.premiumize.me)

## Quick Start

### Docker (Recommended)

```yaml
services:
  decypharr:
    image: cy01/blackhole:latest
    container_name: decypharr
    ports:
      - "8282:8282"
      # Optional: NFSv4 (when NFS is enabled in Settings)
      # - "2049:20490/tcp"
      # Optional: SMB — Windows clients require host port 445 (when SMB is enabled in Settings)
      # - "445:1445/tcp"
    volumes:
      - /mnt/:/mnt:rshared
      - ./configs/:/app # config.json must be in this directory
    restart: unless-stopped
    devices:
      - /dev/fuse:/dev/fuse:rwm
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

> Prefer not to self-host? A managed Decypharr instance is available
> via [ElfHosted](https://store.elfhosted.com/product/decypharr/?utm_source=github&utm_medium=readme&utm_campaign=decypharr-readme),
> preconfigured alongside Sonarr/Radarr to route requests to your debrid provider (7-day trial).

## Documentation

For complete documentation, please visit our [Documentation](https://docs.decypharr.com).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for this team's PR workflow and merge policy.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
