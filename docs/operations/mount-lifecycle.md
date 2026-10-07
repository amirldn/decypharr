# Hanwen mount lifecycle recovery

A config reload used to return from teardown after ten seconds while its goroutine could still call `Server.Unmount` on the old path. A replacement backend could mount at that path before old cleanup finished. The old cleanup also treated an existing directory as evidence of a mounted filesystem. This matches a September 22 playback incident where a replacement mount was followed by delayed unmount activity and the media path became empty.

The Hanwen backend now reserves the canonical mount path for the full lifetime of mount creation and cleanup. A timeout returns an error without releasing that reservation. Cleanup runs once, and only successful completion releases the path. Mount errors returning late are cleaned up under the same reservation. The backend refuses an existing mount instead of trying to force-unmount an unknown filesystem.

Unmount clears readiness immediately. Errors propagate through DFS shutdown and manager reset; the CLI exits the failed internal restart rather than initializing another manager over unfinished teardown. This deliberately favors an explicit failed restart over a process that reports success with a missing media mount. A failed cleanup retains the reservation until process exit. It does not automatically unmount or repair a stale mount from another process.

This reservation coordinates backend instances inside one process. It is not a cross-process lock; independent processes must not deliberately mount the same path concurrently. Existing-mount checks read the OS mount table before touching the configured mount directory. They are not a global mount namespace ownership mechanism.

## Verification

Run the focused tests and race detector on Linux:

```sh
go test ./pkg/mount/dfs/... ./pkg/manager ./cmd/decypharr
go test -race ./pkg/mount/dfs/backend/hanwen
go vet ./pkg/mount/dfs/... ./pkg/manager ./cmd/decypharr
```

The opt-in FUSE test uses an in-memory canary and no debrid provider. Run it only in an isolated Linux mount namespace with `/dev/fuse` and mount capability:

```sh
DECYPHARR_FUSE_TEST=1 go test ./pkg/mount/dfs/backend/hanwen -run TestFUSE -v -timeout 40s
```

It holds real cleanup after a caller timeout, verifies a replacement cannot acquire the path, then checks five new mount/read/unmount generations. Backend tests separately exercise late mount results and blocked VFS close through the real lifecycle code with fake OS-mount operations. The exact historical outage has not been replayed against production.

## Deployment and rollback

Deployment is separately authorized and tracked from implementation. Preserve the currently installed binary, its checksum, build marker, and configuration ownership before replacing anything. Do not reuse an in-process config-save restart for recovery. Inspect DUMB dependencies and active playback first; use its process lifecycle API in a detached job and wait for the old process to exit before starting the replacement.

Acceptance requires a real FUSE mount, a bounded read of an existing movie and episode, a successful Jellyfin media probe, and ingress health. A process health response alone is insufficient. If startup refuses an existing mount, inspect that mount and its owner rather than automatically force-unmounting it.

Rollback restores the saved binary/build marker and uses the same controlled process restart and acceptance checks. No provider requests, library scans, or production settings changes are part of the isolated regression test.
