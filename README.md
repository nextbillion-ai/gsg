# Gsutil Go (gsg)

This is a tool for operating objects on gcs/s3 with command line mode.

-gcs auth:
`GOOGLE_APPLICATION_CREDENTIALS`

-s3 auth:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

-oci copy wait:
`GSG_OCI_COPY_MAX_WAIT`

A copy between two `oci://` locations is OCI's server-side CopyObject, the
same call `oci os object copy` makes, and gsg waits for its work request as
`--wait-for-state COMPLETED` does. The wait is at least the CLI's 20 minutes
and grows with the object (its size at 10 MB/s): large objects copy at tens of
MB/s, and a 69 GB one took 18.5 minutes. `GSG_OCI_COPY_MAX_WAIT` overrides it,
as a Go duration (`90m`) or whole seconds (`5400`), like `--max-wait-seconds`.

`rsync` between two `oci://` locations compares objects by content (size, then
the listed MD5, then CRC32C) rather than by modification time, because OCI's
copy gives the destination a new modification time but keeps its checksums.

-cache directory:
`GSG_CACHE_DIR`

`rsync` compares a local file against a remote object by CRC32C, which means
reading the local file in full. The result is cached, keyed by path and mtime,
so an unchanged file is only ever read once — but the cache lives in `/tmp`,
which in a container is the ephemeral layer: it is thrown away on every restart
and is not shared between two containers of the same pod. Set `GSG_CACHE_DIR`
to a directory on a persistent disk and the cache survives both, so a restart
over unchanged data no longer re-reads it. Unset, it stays `/tmp`.

Only the checksum cache moves. Lock generation caches stay in `/tmp`: their
name comes from the locked object alone, so putting them on a shared volume
would let one process release another's lock. (`/tmp` is container-local, not
process-local, so two processes in one container can already collide there —
the point is not to widen that to every process mounting the volume.)

A directory shared by several processes is only safe when they all see the same
filesystem at the same paths — as two containers mounting one volume do. The
key is a path and an mtime, so processes that mean different files by the same
path would read each other's checksums.
