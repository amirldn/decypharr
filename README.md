# Decypharr

> ## About this fork (amirldn/decypharr)
>
> This is a personal build of [sirrobot01/decypharr](https://github.com/sirrobot01/decypharr), running on a
> Saltbox host with **TorBox only**, an **rclone** mount (`vfs_cache_mode full`) and Plex. `main` is what that
> host runs. Everything below this box is upstream's README.
>
> **Base:** upstream **v2.7.2** (`d6f8df0`). Rebased from v2.6 on 2026-11-14; where upstream had since
> shipped the same fix, upstream's code replaced ours and our tests were kept. Details:
> [`docs/operations/v2.7.2-rebase.md`](docs/operations/v2.7.2-rebase.md).
>
> **What this fork adds on top of upstream:**
>
> 1. **KGBos TorBox reliability changes** (integrated from [KGBos/decypharr](https://github.com/KGBos/decypharr)
>    `71ece25`; details in [`docs/operations/kgbos-v26-integration.md`](docs/operations/kgbos-v26-integration.md)):
>    - TorBox backpressure, with cooldown state that survives restarts
>    - a shared `/requestdl` budget with **priority lanes** (playback before probe and background)
>    - lazy, budgeted CDN-link resolution and caching, cached-only preflight and negative caching
>    - submission mediation and wider retryable-error handling
>    - slow-stream detection with CDN link swapping
>    - capped cache-warm concurrency, so season packs don't trip TorBox 429s
> 2. **Local fixes:**
>    - a VFS directory refresh between symlink retries
>    - downloads reported complete before the pre-cache warm, which now runs in the background; qBittorrent
>      add requests give up after 30s with a retryable 504
>    - queue cleanup waits 15 minutes before blocklisting import problems, and keeps partly imported
>      downloads in the client
>    - entries added in the last 30 minutes are kept when TorBox's list lags behind
>    - diagnostic logging for WebDAV 404s and torrent refreshes
> 3. **Edge cache, a persistent head/tail store** (`pkg/edgecache`, PR #2). It keeps the first 16 MB and last
>    4 MB of every media file on local disk, so player and transcoder start-up reads never wait on a cold
>    TorBox fetch, however long ago a title was last played.
>    - **Why:** a size-capped LRU cache (rclone VFS or DFS) evicts whole files by last access. On a full cache
>      disk that keeps about a day of history, and one measured cold start spent ~8s of ~10.5s probing over
>      TorBox.
>    - **Effect:** start-up reads through the mount take **0.04s stored vs 3–9s cold**.
>    - **How it fills:** at import, through a paced background backfill, and by capturing playback reads.
>      Fills use the background request lane and pause while anyone is streaming.
>    - **Config:** `edge_cache` (`enabled`, `dir`, `head_size`, `tail_size`, `backfill`, `backfill_delay`).
>      Off by default.
>    - It also adds `request.PlaybackUnlessSet`, so stream openers default to the playback lane but respect a
>      background lane the caller set explicitly.
>
> **Branches:**
> - `main`: the deployed build.
> - `rebase/v2.7.2`: this rebase onto upstream v2.7.2.
> - `feat/async-warm-add-timeout`, `feat/edge-cache`, `integrate/kgbos-v2.6`: the previous v2.6-based build
>   and its layers.
> - `archive/main-2026-10-11`: the pre-v2.6 custom build on upstream `main`.
>
> **Building:** `docker build --build-arg VERSION=2.7.2-custom-<sha> --build-arg CHANNEL=stable -t amirldn/decypharr:2.7.2-custom-<sha> .`
> Images are built and deployed locally. They are not published to a registry.

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
