# Known issues

Defects found while investigating a set of intermittent crashes in August 2026,
which are understood but not yet fixed. Each entry says what is wrong, where,
what it costs, and what a fix would involve.

The crashes themselves were fixed separately: #34 (truncated crc32c cache), #35
(`du` slice panic), #36 (progress bar races), #37 (local listing), #38
(truncated lock cache) and #39 (unbounded S3 fan-out). `uat.sh` gained coverage
for all of them in #40.

Everything below was re-checked against `main` after #34 was merged.

---

## 1. A GCS upload reports success even when nothing was stored

`GCS.Upload` in `gcs/gcs.go` ends with:

```go
defer func() { _ = wc.Close() }()
return nil
```

`Close` is what finalizes a GCS upload and where the server's answer arrives;
`io.Copy` succeeds regardless, because at that point nothing has been committed.
So the one place the outcome is reported is discarded, and `defer` runs after
the return value is already set, so even keeping the error could not change it.
`Upload` cannot fail for any reason other than a local read error.

Observed against a real bucket: uploading an object whose name contains a
newline printed `Done (16.0B, 0s, 36.0kB/s)` and exited 0, while GCS had
rejected it with `400: Disallowed unicode characters present in object name`.
Nothing was stored.

This is not specific to odd names. Any failure that surfaces at commit -- a
name over 1024 bytes, a quota, a permission that allows starting but not
finalizing an upload, a network drop while committing a large file -- is
swallowed the same way. In `rsync` the retry wrapper never fires either, since
there is no error to retry on, and with `-d` a source file that silently failed
to upload can then be treated as absent on the next pass.

`S3.Upload` checks its `PutObject` error, so this is GCS only.

**Fix:** capture the `Close` error and return it instead of deferring it. Small
change, but it turns uploads that currently "succeed" into loud failures, so it
deserves its own change and a careful look at what starts failing.

## 2. S3 listing stops early when a page has only common prefixes

`listObjectsAndSubPaths` in `s3/s3.go` paginates with:

```go
if len(lo.Contents) == 0 {
    break
}
...
li.StartAfter = objects[len(objects)-1].Key
```

A non-recursive listing sets a delimiter, so a page can carry `CommonPrefixes`
and no `Contents`. That ends the loop and the remaining pages are never read, so
a prefix with many subdirectories lists short with no error. Combined with
`rsync -d`, an under-listed source means deleting destination files that should
have been kept.

`StartAfter` is also the wrong pagination key; `ContinuationToken` (or the
paginator) is what the API expects.

**Fix:** paginate on `NextContinuationToken`, and end the loop on
`IsTruncated`, not on an empty `Contents`. Needs a real bucket with more than a
page of subdirectories to test against.

## 3. `S3Attrs` reports every failure as "not an object"

`S3Attrs` in `s3/s3.go` returns `(nil, nil)` both when the key genuinely is not
an object and when `GetObjectAttributes` fails for any other reason --
throttling, auth, network. Callers cannot tell the two apart.

`IsObject` and `FileType` depend on nil meaning "not an object", so the contract
cannot simply be changed. `List` and `DiskUsage` (#35) and `batchAttrs` (#39)
skip and log the nil entries rather than dereferencing them, which stops the
crashes but still silently under-reports.

**Fix:** distinguish a not-found response from a real error, propagate the
latter, and keep the nil result only for genuine absence.

## 4. `GetFileCRC32C` cannot report an error, so it returns a wrong value

`common.GetFileCRC32C` returns a bare `uint32`. When a file cannot be read in
full, #34 stops the partial checksum from being written to the cache -- so a
transient error no longer becomes permanent -- but the wrong value is still
returned, because the signature has nowhere to put an error.

Callers only compare it for equality, so the usual outcome is conservative: a
re-copy, or a failed verification. Not always, though:

- `S3.Download` never verifies a download at all. It takes `forceChecksum` but
  uses it only to set `ChecksumMode` on the `GetObject` request, and
  `S3.MustEqualCRC32C` is defined and called from nowhere. `gsg cp -v` from S3
  checks nothing. This is a bug in its own right.
- GCS and S3 attribute lookups return no attributes on failure, so a remote CRC
  defaults to 0. If the local side also returns 0, as it does when the file
  cannot be opened, the comparison passes.
- A wrong partial checksum can collide with the destination's and let
  `Attrs.Same` skip a copy that was needed. Unlikely, not impossible.

**Fix:** `GetFileCRC32C` returns `(uint32, error)`, `Attrs.CalcCRC32C` becomes
`func() (uint32, error)`, and `Attrs.Same` either returns an error or treats a
checksum failure as not-same. Touches `common`, `linux`, `system`, `gcs` and
`s3`, and should close the `S3.Download` gap at the same time.

## 5. An S3 unlock deletes whatever lock is there

`S3.DoAttemptUnlock` in `s3/s3.go` accepts an `etag` argument, logs it, and then
calls `DeleteObject` without an `If-Match` condition. So it removes the current
remote lock regardless of whether the caller holds it -- including one another
process acquired a moment earlier. The GCS equivalent conditions its delete on
the generation it stored.

**Fix:** condition the delete on the cached ETag, and fail when it does not
match.

## 6. `du` misparses filenames containing a newline

`Linux.DiskUsage` shells out to `du -aB1` and splits on newlines, so a filename
containing one splits across lines. #37 stops the resulting panic by skipping
lines that do not parse, but the entry is still wrong: the name is truncated at
the newline, and a continuation line that happens to start with digits and
whitespace can be read as a size.

Directory totals are unaffected, since `du -s` prints only the last line.

`du -aB1` is also GNU-only; the `-B` flag does not exist on BSD.

**Fix:** compute usage in Go with `filepath.WalkDir` instead of shelling out.
That fixes both the parsing and the portability, but it reports apparent size
rather than allocated blocks, so the numbers will change.

## 7. A progress bar container can never be stopped

`bar.Container.printer` loops forever with no way to stop it, and the container
writes to `os.Stdout` with no way to redirect it. One goroutine per process, and
`bar.New` is called once, so this leaks nothing in practice. It does make the
package awkward to test: every `bar.New` in a test leaves a goroutine printing
for the rest of the run, and swapping `os.Stdout` around it is itself a data
race.

`Progress`, `Speed` and `CurrentTime` are also still exported while being
guarded by an unexported mutex (#36), so an outside caller can race on them and
cannot reach the lock.

**Fix:** a `Close` or stop channel, plus an injectable writer. Unexporting the
guarded fields would be a breaking change.

## 8. Listing holds every key in memory, on both backends

`GCS.batchAttrs` accumulates every `*storage.ObjectAttrs` into one slice, and
`DiskUsage` then builds a `DUTree` node per object on top of that. On the S3
side `listObjectsAndSubPaths` accumulates all `types.Object` values, then all
sub-paths, then `batchAttrs` builds equally long `res` and `errs` slices. #39
bounded the goroutines and in-flight requests but not the allocation.

Measured: 1M `*storage.ObjectAttrs` with ~40 character keys occupy **511 MB**
(536 bytes each; the struct alone is 456 bytes before string contents). The tree
adds another node and map per object on top. A million-object `du` is
comfortably a gigabyte resident.

Note this is not avoided by the delimiter. `du` always lists recursively
(`cmd/du.go` is the only caller and always passes `recursive=true`), so a
delimiter is never used and nothing is ever collapsed. The API pages the
responses; it is the accumulation that is unbounded.

**Fix:** stream pages to the caller instead of accumulating, which changes the
`ISystem.List` signature.

## 9. S3 fetches attributes it was already given, one call per object

`s3.toAttrs` needs exactly three things: `Size`, `ModTime` and `CRC32`.

`ListObjectsV2` already returns `Size` and `LastModified` for every object, and
`listObjectsAndSubPaths` throws them away:

```go
for _, o := range objects {
    subPaths = append(subPaths, *o.Key)   // Size and LastModified discarded
}
```

`batchAttrs` then issues one `GetObjectAttributes` call per key to fetch back
the size and mtime it just dropped. Only `CRC32C` genuinely requires the extra
call, since the listing does not carry it.

That single decision causes most of what is wrong with the S3 path:

- **The request explosion.** One call per object is what #39 had to put a
  concurrency cap on.
- **Partial failure.** A million independent calls fail independently, and
  `S3Attrs` reports every failure as "not an object" (item 3), so a failed
  lookup silently drops that object from the result. GCS has no equivalent: its
  attributes arrive with the listing, so a failure fails the whole listing and
  `du` exits non-zero rather than returning a short answer.
- **The nil handling.** Every nil check in `List` and `DiskUsage` exists to cope
  with the nil entries this produces.

**Fix, in this order -- the order matters:**

1. Retry the per-object fetch with the existing `common.DoWithRetrySimple`, as
   `s3.Download` already does, so a transient error is not a permanent skip.
2. Then have `S3Attrs` return `(nil, nil)` only for a genuine NoSuchKey and
   propagate everything else. `batchAttrs` already collects an `errs` slice and
   returns the first non-nil entry, so the plumbing exists and the errors simply
   never arrive. This makes S3 fail as loudly as GCS.

   Not the other way round: with a million independent calls, even a 0.01%
   transient failure rate means roughly a hundred failures per run. Made fatal
   without retries first, `du` over a large prefix would fail every time.
3. Then stop making the calls. Build `Attrs` from the list response and set
   `Attrs.CalcCRC32C` to a closure that fetches the checksum for that one key on
   demand. The hook already exists and the linux backend already uses it exactly
   this way, so that `ls` never hashes a file it was only asked to list.

   That takes `ls`, `du`, `cat` and `rm` to zero per-object calls. `rsync` needs
   one more change to benefit: `Attrs.Same` calls `CalcCRC32C` unconditionally
   whenever it is set, even without `-v`, so it should short-circuit when the
   sizes already differ.

**Unverified, and worth checking first:** `s3.Upload` does a plain `PutObject`
with no checksum algorithm requested, so objects gsg uploaded may carry no
stored CRC32C at all. If so, `crc32c` is 0 on both sides and the comparison is
already a no-op, which changes how much of step 3 is even needed.

## 10. A non-recursive `du` reports zero for every directory

With `recursive=false` the listing uses a delimiter, so subdirectories come back
as common prefixes: a path and no size. `DiskUsage` adds them to the tree so
they appear in the output, but nothing can fill in their size, and the objects
underneath were never listed.

Measured against a prefix holding `top.txt` (5 bytes) and a subtree of 19000
bytes:

```
DiskUsage(recursive=true)         DiskUsage(recursive=false)
  5      nr/top.txt                 0      nr/sub/
  ...                               5      nr/top.txt
  19000  nr/sub/                    5      nr/
  19005  nr/
```

The subtree reports 0 and the root total is wrong. This is inherent to a
delimiter listing -- the size is not in the response, and computing it means
walking the subtree, which is what the delimiter exists to avoid.

Unreachable today: `cmd/du.go` is the only caller and always passes
`recursive=true`. Reachable through `ISystem.DiskUsage` directly.

**Fix:** either have `DiskUsage` reject `recursive=false`, or descend per prefix
to get real subtotals, which defeats the point of the delimiter. Rejecting it is
probably right.

## 11. S3 ignores the size of an object whose key ends in "/"

`batchAttrs` short-circuits every sub-path ending in `/`:

```go
if strings.HasSuffix(subPath, "/") {
    res[index] = &S3Attributes{
        S3Attrs: &s3.GetObjectAttributesOutput{},   // empty: no ObjectSize
        Bucket:  bucket,
        Prefix:  subPath,
    }
    continue
}
```

so the size is never fetched and reads back as 0. That is right for a common
prefix, which has no size and is not an object at all. It is wrong for a real
object whose key happens to end in `/` -- the directory markers that console
UIs and Hadoop write. Their bytes are silently missing from `du`.

Measured: a 7 byte marker added to a prefix already holding 27 bytes leaves
`du -s` reporting 27.

The short-circuit exists because `listObjectsAndSubPaths` flattens two different
things into one `[]string`: real keys from `Contents`, and synthetic entries from
`CommonPrefixes`. Once flattened, a trailing `/` is the only thing left to tell
them apart, and it cannot. Note that in a recursive listing there are no common
prefixes at all, so every trailing-slash entry there is a real object and the
short-circuit is always wrong.

Rare in practice, since markers are almost always zero length. Pre-existing, and
`uat.sh` pins the current behaviour so that changing it is deliberate.

**Fix:** keep the two kinds apart instead of flattening them -- which is the same
change item 9 needs, since it also wants `Size` and `LastModified` carried
through from the listing rather than refetched.
