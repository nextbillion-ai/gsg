# Gsutil Go (gsg)

This is a tool for operating objects on gcs/s3 with command line mode.

-gcs auth:
`GOOGLE_APPLICATION_CREDENTIALS`

-s3 auth:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

-cache directory:
`GSG_CACHE_DIR`

`rsync` compares a local file against a remote object by CRC32C, which means
reading the local file in full. The result is cached, keyed by path and mtime,
so an unchanged file is only ever read once — but the cache lives in `/tmp`,
which in a container is the ephemeral layer: it is thrown away on every restart
and is not shared between two containers of the same pod. Set `GSG_CACHE_DIR`
to a directory on a persistent disk and the cache survives both, so a restart
over unchanged data no longer re-reads it. Unset, it stays `/tmp`.

Only the checksum cache moves. Lock generation caches stay in `/tmp`, where
being process-local is what keeps one process from releasing another's lock.

A directory shared by several processes is only safe when they all see the same
filesystem at the same paths — as two containers mounting one volume do. The
key is a path and an mtime, so processes that mean different files by the same
path would read each other's checksums.
