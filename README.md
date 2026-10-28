# Decypharr

> ## About this fork (amirldn/decypharr)
>
> This is a personal build of [sirrobot01/decypharr](https://github.com/sirrobot01/decypharr), running on a
> Saltbox host with **TorBox only**, an **rclone** mount (`vfs_cache_mode full`) and Plex. `main` is what that
> host runs. Everything below this box is upstream's README.
>
> **Base:** upstream **v2.6** (`3a0e0e2`).
>
> **What this fork adds on top of v2.6:**
>
> 1. **KGBos TorBox reliability changes** (integrated from [KGBos/decypharr](https://github.com/KGBos/decypharr)
>    `71ece25`; details in [`docs/operations/kgbos-v26-integration.md`](docs/operations/kgbos-v26-integration.md)):
>    - TorBox backpressure, with cooldown state that survives restarts
>    - a shared `/requestdl` budget with **priority lanes** (playback before probe and background)
>    - resolved-CDN-link caching, cached-only preflight and negative caching
>    - submission mediation and wider retryable-error handling
>    - slow-stream detection with CDN link swapping
>    - capped cache-warm concurrency, so season packs don't trip TorBox 429s
> 2. **Local fixes:**
>    - a VFS directory refresh between symlink retries
>    - category-less qBittorrent login (the Arr's URL as username, its API key as password)
>    - refetching links on HTTP 400 (already upstreamed via KGBos)
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
> - `main` / `feat/edge-cache`: the deployed build.
> - `integrate/kgbos-v2.6`: v2.6 + KGBos + local fixes, without the edge cache.
> - `archive/main-2026-10-11`: the previous `main` (pre-v2.6 custom build on upstream `main`). It also
>   carries two post-v2.6 **upstream** fixes, `66c2cb5` (Arr authentication and recovery submissions) and
>   `26f9f51` (Radarr manual-import payloads). They were left out on purpose: this build already has its own
>   category-less Arr login, and neither bug appeared in the deployed logs. They will come back with the next
>   upstream rebase.
>
> **Building:** `docker build --build-arg VERSION=2.6-custom-edge-<sha> --build-arg CHANNEL=stable -t amirldn/decypharr:2.6-custom-edge-<sha> .`
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
