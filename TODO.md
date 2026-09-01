# Known issues

Defects found while investigating a set of intermittent crashes in August 2026.
Each entry says what is wrong, where, what it costs, and what a fix would
involve.

The crashes themselves were fixed separately: #34 (truncated crc32c cache), #35
(`du` slice panic), #36 (progress bar races), #37 (local listing), #38
(truncated lock cache) and #39 (unbounded S3 fan-out). `uat.sh` gained coverage
for all of them in #40.

Every item below was then reproduced, or an attempt was made, against real
buckets. That changed the picture: some are worse than first written, and
several are not worth fixing. The evidence is recorded under each one.

| # | Status | Reproduced? | Verdict |
|---|--------|-------------|---------|
| 1 | PR #41 | yes -- cp exits 0 having stored nothing | fix |
| 2 | PR #45 | yes -- 1000 of 1005 subdirectories listed | fix |
| 3 | PR #46 | yes -- a 301 is reported as "not an object" | fix, with 14 |
| 4 | PR #47 | yes -- `cp -v` from s3 logs 0 checksum checks | fix |
| 5 | PR #44 | yes -- unlock deleted another holder's lock | fix |
| 6 | open | yes, but needs a newline in a filename | low |
| 7 | open | no -- one goroutine per process | low, testing annoyance only |
| 8 | open | synthetic only -- 511 MB per million objects | low unless such prefixes exist |
| 9 | PR #62 | yes -- 2.1x slower, ~1006 extra calls per 1006 objects | fixed |
| 10 | open | yes, but unreachable from the CLI | low |
| 11 | PR #62 | yes, but needs a marker carrying bytes | fixed, with 9 |
| 12 | PR #42 | yes -- 8 data races under -m | fix |
| 13 | PR #43 | n/a -- duplication, one copy with a flaw | fix |
| 14 | PR #46 | yes -- 301 MovedPermanently across regions | fix, with 3 |
| 15 | PR #44 | yes -- 8 of 8 processes acquire the same lock | fix, folded into #44 |
| 16 | open | yes -- one receipt file for gs and s3; oci is clear | low, needs the same bucket and key on both |
| 17 | deferred | yes -- a 6-object promotion landed 1 object, exit 1; oci verified clear | small fix, deferred -- s3 is not the current focus |
| 18 | open | yes -- measured: non-gsg objects re-download on every rsync | low, owner's call -- costs work, not correctness |
| 19 | PR #63, #64 | yes -- s3 rejected >5 GiB outright; oci was capped by link speed | fixed |
| 20 | PR #59 | yes -- `du` and `cp -r` exited 1 printing nothing at all | fixed: common.ExitWith |
| 21 | PR #58 | yes -- and `mv -r d d/sub` took two objects to none | fixed in cmd/mv.go |
| 22 | PR #61 | yes -- 237ms on s3, 198ms on gs, to answer one boolean | fixed |
| 23 | PR #57, #60 | yes -- a gs upload is stored with a checksum of whatever arrived | fixed on gs and oci |
| 24 | open | yes -- A releases B's lock after its own expired, on one machine | fix, design change |

Suggested order for what remains: 15, then 2, then 14 and 3 together, since
fixing 3 alone converts silence into errors without making the requests
correct.

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

**Reproduced.** 1005 subdirectories under one prefix, each holding one object,
with no object at the top level so the first page carries only common prefixes:

```
provider reports: 1005 subdirectories
gsg ls          : 1000        <- exactly MaxKeys
gsg ls -r       : 1005        <- recursive uses no delimiter, so Contents is never empty
```

The same fixture on gs lists 1005 of 1005: the GCS iterator paginates for us.
S3 only.

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

**Reproduced, and it hides item 14.** Asking gsg about an object in a bucket
whose region the cached client does not match:

```
gsg (IsObject)          -> ok=false err=<nil>
the same call, unwrapped -> 301 MovedPermanently
```

So a request that failed outright is reported as "this is not an object". Any
caller then treats the object as absent. Fixing 14 without fixing this would
still leave every other failure -- throttling, auth, a network blip -- silently
reported as absence.

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

**Reproduced for the S3 half.** Downloading the same object from each backend
with `cp -v`, counting log lines containing "CRC32C checking":

```
gs: 2        s3: 0
```

`-v` on the s3 path verifies nothing at all. The "returns a wrong value" half
could not be reproduced: forcing a read to fail partway through a regular file
is not something a test can arrange portably.

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

**Reproduced, low value.** A file named `we<newline>ird.txt` makes `du -aB1`
emit a record split across two lines, and the entry comes back truncated to the
part before the newline. The panic this used to cause is fixed (#37); what
remains is a wrong name in `du` output, and only for filenames containing a
newline.

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

**Not reproduced, and probably not worth fixing.** `bar.New` is called once per
process, so the immortal goroutine is not a leak in any real run. The cost is
that the package is awkward to test, which is a reason to change it only if
someone is working there anyway.

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

**Only measured synthetically.** 511 MB for a million `*storage.ObjectAttrs`
with realistic keys, plus a tree node each. Whether that matters depends on
whether prefixes of that size actually get listed; nothing here demonstrates
that they do.

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

**Reproduced, modest at this size.** `ls -r` over 1005 objects, three runs each:

```
gs: 0.50s 0.53s 0.53s
s3: 1.20s 0.92s 0.91s
```

About 1.8x, and roughly 1005 extra API calls that return data the listing
already carried. Real money and latency at a million keys; barely visible at a
thousand. The stronger argument for fixing it remains the partial-failure
surface it creates, not the speed.

**Fixed in PR #62.** All three steps, in the order above. Measured over 1006
objects: `ls -r` 0.88-1.05s -> 0.45s, `du` 1.06s -> 0.49s, output byte for byte
identical, and the per-object requests gone rather than merely capped.

Two wrinkles worth recording. Step 3 moves the checksum onto `Attrs.CalcCRC32C`,
and the first draft fetched one checksum per call -- a serial round trip inside
rsync's comparison loop, where the code it replaced had used a bounded parallel
fan-out. Measured over the same 1006 objects, that made rsync 64.4s -> 93.0s,
giving back more than the listing win. The fix is to keep the fan-out but defer
it: the first checksum read fills the whole listing's worth at once, with the
same cap. rsync is then 64.7s against 64.4s, and `ls`, `du`, `cat` and `rm`
never trigger it at all.

The second wrinkle was a real bug, caught in review. Deferring the checksum
means the fetch can come back with nothing -- the object carries no comparable
CRC32C, or the request failed -- and `CalcCRC32C` returned a bare `uint32`, so
either became a checksum of 0 that `Attrs.Same` compared as if it were real.
Before the change a failed fetch aborted the whole listing instead, so this was
new. Both sides have to come back empty for it to bite, which rules out the
local cases but not a cloud-to-cloud rsync: with `-v` the modification time is
skipped by design, so two same-sized objects that both lack a checksum compared
*equal*. Measured: `gsg rsync -r -v s3://.../src s3://.../dst` over 4-byte
objects holding `AAAA` and `BBBB`, neither carrying a CRC32C, left the
destination as `BBBB` and reported no diff.

`CalcCRC32C` now returns `(uint32, bool)` and `Same` treats "could not
determine" as a difference, so the object is copied rather than skipped on the
strength of two failures agreeing. That is the conservative answer rather than
the loud one -- step 2's loudness cannot be kept once the fetch happens after
the listing has been returned -- and it is the safe direction: a needless copy,
never a silent skip. This is a narrower version of item 18.

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

**Reproduced, but unreachable.** `cmd/du.go` is the only caller and always
passes `recursive=true`, so nothing in the CLI can hit this. It is reachable
only through `ISystem.DiskUsage` directly.

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

**Reproduced, low value.** A 7 byte marker added to a prefix holding 27 bytes
leaves `du -s` reporting 27. Directory markers are essentially always zero
length, so the undercount needs an unusual object to appear at all.

**Fixed in PR #62,** as predicted, by item 9's change: carrying `Size` through
from the listing keeps real keys and common prefixes apart, so a marker is
counted like the object it is. `aws s3 ls --recursive --summarize` over the
same three objects reports 27 where gsg reported 20; both now say 27.

## 12. Both cloud backends race on their lazy client

`GCS.Init` and `S3.Init` are check-then-set with no synchronization:

```go
func (g *GCS) Init(_ ...string) error {
    if g.client != nil {          // read
        return nil
    }
    ...
    g.client, err = storage.NewClient(...)   // write
```

The backends are process-wide singletons -- `cmd/root.go` registers one
`&gcs.GCS{}` and one `&s3.S3{}` -- and every worker goroutine calls `Init` at
the top of whatever it is doing. With `-m` they race.

Found by building `main` with `-race` and running `gsg -m cp -r` of 40 files at
a real bucket: twelve races reported, with `gcs.Init` among them at both the
read and the write.

The likely outcome is two clients being built and one leaked, since a pointer
write is not torn on the architectures gsg targets. That is still undefined
under the Go memory model, and it is the kind of thing that stops being benign
when a future client type grows more state.

**Fix:** a `sync.Once` per backend. Note `S3.Init` also takes a bucket argument
and derives the region from it, so its Once has to key on something or the
first bucket seen wins -- which is arguably already the behaviour, since the
client is cached after the first call.

**Also:** `uat.sh` gained `GSG_UAT_RACE=1`, which builds with the race detector
and aborts on any race. It cannot be turned on in earnest until this and item 7
are fixed.

## 13. Two copies of the atomic-write logic, and the older one is worse

`common.WriteFileAtomic` (added with the lock cache fix) and
`common.writeCRC32cCache` (added with the crc32c cache fix) do the same thing:
create a temp file beside the target, write, chmod, sync, close, rename. They
live in the same package, a few hundred lines apart.

They are not identical, and the difference matters. `writeCRC32cCache` chmods
**before** writing:

```go
if err = cf.Chmod(0644); err != nil { ... }
if _, err = cf.Write(crcBytes); err != nil { ... }
```

so between those two calls the temp file is group and world readable while
still empty or partial. A process killed there leaks a short, widely readable
file. `WriteFileAtomic` was reviewed later and writes first, chmods second, for
exactly that reason. The older copy never got the correction because it had
already merged.

`writeCRC32cCache` also swallows every error and logs at debug, so a caller
cannot tell whether the cache was written; `WriteFileAtomic` returns the error.

**Fix:** reduce `writeCRC32cCache` to marshalling four bytes and calling
`common.WriteFileAtomic(cacheFileName, crcBytes, 0644)`, keeping its debug log
on the returned error. That deletes roughly thirty lines and removes the
chmod-ordering flaw. Note the modes differ on purpose -- 0644 for the crc32c
cache, since a cache written by one user staying readable by another is a real
saving, and 0600 for the lock caches, where cross-user unlock cannot work
anyway -- so the parameter stays.

Worth checking at the same time whether the lock-generation encode/decode
should be shared too: gcs and linux both marshal a uint64 the same way and both
guard the length on the way back.

## 14. One S3 client for the process, with the region taken from the first bucket

`S3.Init` caches a single client and derives its region from whichever bucket
happens to call it first:

```go
region, err = s3manager.GetBucketRegion(ctx, sess, bucket, "ap-southeast-1")
```

Every later call returns that client regardless of which bucket it was asked
about, so operating across two regions in one process uses the wrong endpoint
for one of them. Guarding the lazy init with a mutex made this deterministic
rather than racy; it did not make it correct.

The region lookup also swallows its own error -- the callback returns `nil` on
failure rather than propagating -- so a transient lookup failure caches a client
built with the fallback region instead of failing.

**Fix:** cache clients keyed by region (or by bucket), and treat a failed region
lookup as a failed `Init`. If one region per process is the intended invariant,
say so and enforce it rather than leaving it to call order.

**Reproduced, and this account is exposed to it.** Buckets here span seven
regions: ap-southeast-1 (20), us-west-2 (7), us-east-2 (3), eu-central-1 (2),
ap-south-1 (2), us-west-1, ap-southeast-2, and four in us-east-1.

A HeadObject against a us-west-2 bucket:

```
client pinned to us-west-2      -> 404 NotFound          (correct: the key is absent)
client pinned to ap-southeast-1 -> 301 MovedPermanently  (the request fails)
```

So any single gsg process touching two buckets in different regions -- a
cross-bucket `cp`, an `rsync` between buckets -- uses the wrong endpoint for
one of them. Item 3 then turns the 301 into "not an object".

## 15. Acquiring an S3 lock is not mutually exclusive

`S3.DoAttemptLock` decides whether it may take the lock, then takes it, with
nothing binding the two together:

```go
_, err = s.client.HeadObject(...)          // is there a lock?
... if expired ...
_, _ = s.client.DeleteObject(...)          // unconditional: may delete a NEW holder's lock
...
putOutput, err := s.client.PutObject(...)  // unconditional: overwrites whoever got there first
```

Two contenders can both come through it holding what each believes is the lock.
The expired-lock cleanup deletes unconditionally, so a lock taken between the
Head and the Delete is destroyed; and the create overwrites rather than failing
when the object already exists.

The gcs backend does neither: it creates with `DoesNotExist: true`, and its
expired-lock cleanup deletes with `GenerationMatch`.

Raised while reviewing the fix for item 5, which conditioned the *release* on
the caller's ETag. Release is now safe on AWS; acquire is not, so S3 locking is
still not a correct distributed lock.

**Fix:** `IfNoneMatch: "*"` on the create, treating 412 as "not acquired", and
`IfMatch` with the observed ETag on the expired-lock delete. Both are the same
conditional-request feature item 5 uses, so they carry the same question about
providers that ignore or reject it -- see the note there.

**Reproduced, and worse than described above.** Eight processes racing for the
same lock, three rounds:

```
s3: 8 of 8, 6 of 8, 8 of 8 processes believe they hold the lock
gs: 1 of 8, 1 of 8, 1 of 8
```

Not "two contenders can both come away holding it" -- essentially all of them
do. S3 locking provides no mutual exclusion worth the name. Anything relying on
it for exclusion has none. This is the first thing to fix.

## 16. A lock receipt is shared between the two schemes

`common.GenTempFileName(bucket, "/", object)` hashes the bucket and the object
and nothing else, so a lock on `gs://b/x.lock` and one on `s3://b/x.lock` write
to the same file in /tmp:

```
gs://gsg-uat/same.lock -> /tmp/d3abc4d6c4e06f8533531ed2a44ae948
s3://gsg-uat/same.lock -> /tmp/d3abc4d6c4e06f8533531ed2a44ae948
```

Taking the second lock overwrites the first's receipt, and the first can then no
longer be released -- it will be refused, since the receipt now holds the other
backend's identifier, and the lock stands until its TTL.

**gs and s3 only.** `oci/lock.go` names its receipt
`GenTempFileName("oci", "://", bucket, "/", object)`, so it cannot collide with
either of the others. The fix for gs and s3 is to do the same.

**Reproduced** while testing item 15, by racing locks on both schemes at the
same bucket and key. It needs a bucket of the same name to exist on both
providers and the same key used for a lock on each, which is why it is filed
low rather than as a defect anyone is likely to hit.

**Fix:** include the scheme in the name passed to `GenTempFileName`. Note that
changes every receipt path, so receipts written by an older gsg become
unreadable and their locks wait out the TTL -- the same rollout consideration as
item 15's legacy-receipt handling.

## 17. A cross-bucket copy silently drops keys with awkward characters

`S3.Copy` builds the source reference by concatenation:

```go
CopySource: aws.String(fmt.Sprintf("%v/%v", srcBucket, srcPrefix)),
```

AWS requires that value URL-encoded. Unencoded, a key carrying a character
that means something in a URL is misread, and the copy does not happen.

S3 **URL-decodes** that header, so `+` becomes a space, `%20` becomes a space,
and raw non-ascii is not valid in a header at all. S3 then looks up a key nobody
wrote and truthfully reports it missing.

**Re-reproduced after #46** (the earlier note here was wrong on two counts:
`#` copies fine, and the failures are not silent -- they surface as NoSuchKey):

```
"plain.txt"        landed: yes
"with space.txt"   landed: yes
"hash#tag.txt"     landed: yes   <- earlier note said no
"café.txt"         landed: NO    NoSuchKey
"plus+sign.txt"    landed: NO    NoSuchKey   <- + decodes to space
```

Scope: s3 -> s3 only, including within one bucket. Cross-cloud copies go through
`interCloudCopy`, which downloads and re-uploads and never builds this header.
`GCS.Copy` uses typed object handles, so it cannot have the bug.

**oci verified clear.** `CopyObject` takes the source name as a typed field
rather than a concatenated header, and an oci -> oci `cp -r` over the same six
awkward names copied 6 of 6, including `café.txt`, `plus+sign.txt` and
`run=2026-08-23T02:00:00+05:30.txt`.

**What it costs, on a realistic tree.** Promoting a staging export to prod,
where a Hive partition is named by an ISO timestamp -- IST puts `+05:30` in the
key -- and a POI file has an accent:

```
exports/run=2026-08-23T02:00:00+05:30/{roads,places}.parquet, _SUCCESS
poi/mumbai/café-leopold.json
poi/mumbai/gateway-of-india.json
poi/delhi/khan-market.json
```

`gsg -m cp -r` landed **1 of 6** and exited 1. The whole operator-visible output
was one line: `NoSuchKey: The specified key does not exist` -- which does not
name the key, and reads as a lie, since the key is plainly there in staging.

Note the blast radius exceeds the bad keys: `gateway-of-india.json` is plain
ascii and did not land either, because gsg aborts on the first error while the
other copies are still in flight under `-m`. Which objects survive is a race.
The destination is left partially written rather than empty or complete.

**The silent variant.** If a key exists that is the URL-decoding of another key,
S3 finds it and the copy succeeds with the wrong contents:

```
src  a%20b.txt -> "I am the LITERAL percent key"
src  a b.txt   -> "I am the SPACE key"
after cp -r:
dst  a%20b.txt -> "I am the SPACE key"      wrong object, exit 0, no error
```

Rarer, since it needs both keys to exist, but it is why this is a correctness
bug rather than an ergonomics one.

**GCS is immune, verified by running the same case.** The identical key set
copied gs -> gs landed 6 of 6 at exit 0, and `a%20b.txt` kept its own contents:

| | s3 -> s3 | gs -> gs |
|---|---|---|
| 6-object tree with `+05:30`, `café`, `%20` | 1 landed, exit 1 | 6 landed, exit 0 |
| `a%20b.txt` contents | wrong object, silently | correct |

Two independent reasons. `GCS.Copy` passes the key as a typed value the client
library encodes itself, so no string is assembled; and the GCS copy API takes
source bucket and object as separate fields rather than one composite header.

**This is the only such place in the s3 backend.** Every `Sprintf` and every
non-trivial `aws.String(...)` in `s3/s3.go` was checked: `CopySource` here,
`Range: bytes=%d-%d` (numbers only), and `prefix + match[1]` (a result string,
not an API value). Everywhere else the key is a plain parameter the SDK encodes,
which is why `ls`, `du`, `cat`, upload and download all handle these names
fine. So an s3 -> s3 copy is the only unsafe operation, whether or not the two
buckets differ.

**Deferred (Aug 2026), repo owner's call:** s3 is not the current focus and the
fix is not worth the effort right now. Nothing about the analysis is
outstanding -- the mechanism, scope and blast radius are all recorded above, so
picking this up later is just the encoding change plus tests. Worth doing before
any campaign that moves data between s3 prefixes or buckets, since that is the
one operation it breaks.

Found while reviewing the region fix; independent of it.

**Fix:** encode the source reference, escaping each path segment and leaving
the separators alone -- `url.PathEscape` on the key would also escape its
slashes, so it cannot be applied to the whole thing at once. Worth checking the
same call's `Key` and the equivalent in `PutObject` while there.

## 18. Attrs cannot say "there is no checksum"

`system.Attrs.CRC32` is a bare `uint32`, so an object that carries no checksum
is indistinguishable from one whose checksum happens to be zero. `Attrs.Same`
compares the number unconditionally:

```go
r = r && a.CRC32 == b.CRC32
```

Two S3 objects that both lack a checksum therefore compare equal and are treated
as identical on size and mtime alone, even under `-v`. Before #47 every S3
object read back as 0, so this was universal; after it, it applies to objects
written by something that stored no CRC32C, or a multipart object whose
checksum is of-parts.

The reverse costs work rather than correctness: an S3 object with no checksum
against a local file always compares unequal, so it is copied again on every
run.

**Fix:** carry presence alongside the value -- a `*uint32`, or a bool beside it
-- and have `Same` skip the comparison when either side has nothing to offer,
rather than comparing zeroes. This is the same shape as the other half of item
4, where `GetFileCRC32C` has no way to say it could not compute one, so the two
are worth doing together.

### Measured after #47, against a real bucket

Three objects of identical content in one prefix, differing only in who wrote
them, then `rsync -r` run three times into the same local directory:

| object written by | checksum S3 holds | re-downloaded every run |
|---|---|---|
| `gsg cp` | `CRC32C`, FULL_OBJECT | no |
| `aws s3 cp` | none | **yes** |
| multipart, 12 MB | none, 2 parts | **yes** |

Stable on runs 2 and 3, so it is permanent rather than a warm-up effect. #47
therefore makes `rsync` incremental only for objects gsg itself uploaded; for a
bucket populated by anything else, every run still transfers everything. That
is still strictly better than before #47, where every S3 object read back as 0
and so nothing was ever incremental.

`-v` handles the same case correctly -- it logs `no CRC32C stored` and skips --
so the gap is specifically in `Same`, not in the verification path.

### Where the missing checksums come from (not a gsg defect)

Worth recording because it decides how much of item 18 is gsg's to fix. S3 does
not compute checksums on its own; it stores what the uploader sends. Measured
with `aws-cli/2.15.30`:

| upload method | stores CRC32C? |
|---|---|
| `aws s3 cp` | no |
| `aws s3 cp` with `AWS_REQUEST_CHECKSUM_CALCULATION=when_supported` | no -- setting postdates this CLI |
| `aws s3api put-object --checksum-algorithm CRC32C` | yes |

With the explicit flag, a second `rsync` does report `No diff detected`, so for
single-part objects this is caller-fixable. Two caveats:

- `aws s3 cp` on 2.15.30 has no checksum flag at all; only `s3api` does. CLI
  v2.23+ (Jan 2025) adds one and defaults to `when_supported` -- but that
  default computes **CRC32, not CRC32C**, which gsg cannot use either. The
  explicit `--checksum-algorithm CRC32C` is needed regardless of version.
- Multipart cannot be fixed this way. A multipart upload with CRC32C on every
  part stored `jDbzjw==` while the file's whole-file CRC32C is `SxGpLQ==` --
  a checksum of the part checksums, for identical bytes. gsg reads
  `ChecksumType="COMPOSITE"` and correctly declines to compare. AWS added
  full-object multipart checksums in Jan 2025, but `create-multipart-upload`
  on this CLI has no `--checksum-type` flag, so on 2.15.30 a multipart object
  cannot carry a comparable checksum at all.

So multipart objects will never have a whole-file CRC32C to compare, which is
the argument for fixing `Same` rather than expecting callers to upload
differently: without it those objects re-copy on every run forever.

### What knowing "there is no checksum" would buy

Presence tracking is not itself the fix; it is what makes a fallback
expressible. Today `Same` has no choice to make. Three become available:

1. **ETag/MD5.** Every S3 object has an ETag, and for a single-part upload it
   is the MD5 of the content. `GetObjectAttributesOutput.ETag` is already
   fetched and ignored, and `common.GetFileMD5` already exists. That covers the
   `aws s3 cp` case with a real content comparison rather than a proxy.
   Multipart ETags carry a `-N` suffix and are not MD5, so they are detectable
   and excluded, the same way COMPOSITE is.
2. **Size plus mtime.** Works because `Download` already stamps the local file
   with the remote mtime, so after the first sync the two agree and the second
   run reports no diff. This is the only option for multipart objects. Note
   `Same` ignores mtime under `-v`, so it covers plain `rsync` only.
3. **Saying so.** Two remote objects that both lack a checksum currently give
   `0 == 0` and compare equal, so a cloud-to-cloud `rsync` treats two different
   files as identical -- including under `-v`. With presence tracking that
   becomes an MD5 comparison or an honest "cannot verify", never a silent pass.

**Priority: low, decided by the repo owner (Aug 2026).** The re-download cost is
real and recurring but it is wasted work, not a wrong answer, and the one
genuine correctness case in (3) needs two checksum-less objects being compared
against each other. Revisit if S3 rsync cost becomes visible, or if
cloud-to-cloud `rsync -v` starts being relied on.

If picked up, (2) and (3) are the mechanical part; (1) is a design decision that
roughly doubles the change and can be a follow-up.

---

## 19. An S3 upload is a single PutObject, so >5 GB fails and nothing resumes

`S3.Upload` and `S3.PutObject` in `s3/s3.go` both send the whole file as one
`PutObject` body. There is no `manager.Uploader` anywhere in the tree, so gsg
never uploads multipart.

Two consequences. S3 caps a single `PutObject` at 5 GB, so a larger file cannot
be uploaded at all -- the API rejects it. And because the transfer is one
request with no part boundaries, a network failure partway through a large
upload restarts from zero rather than resuming.

**Now reproduced, on both s3 and oci -- and they fail differently.**

s3, 6 GiB, single PutObject:

```
exit 1 in 0s, nothing stored
EntityTooLarge: Your proposed upload exceeds the maximum allowed size
```

Immediate, unambiguous, and clean: the service rejects it on the headers before
any bytes move.

oci is worse, because the ceiling is not a size at all. gsg does no multipart
there either, and OCI does not reject on size -- the request simply runs into
the SDK client's 60-second timeout:

| size | result |
|---|---|
| 1 GiB | stored, 38s, correct size and CRC32C |
| 3 GiB | **exit 1 at 61s** -- `Client.Timeout exceeded while awaiting headers` |
| 6 GiB | **exit 1 at 63s** -- same |

**The cause is a fixed wall-clock deadline, not the size and not a flaky link.**
The OCI SDK builds its `http.Client` with `Timeout: 60s`
(`common/client.go:81`), and Go applies that to the *whole* request -- the body
upload included -- so any PUT that takes longer than a minute is cancelled
mid-stream regardless of how healthy the connection is.

Proved by moving the deadline and changing nothing else. The SDK reads
`OCI_CUSTOM_CLIENT_TIMEOUT` (seconds):

| file | default 60s | raised |
|---|---|---|
| 3 GiB | fails at 61s | **stored**, 76s @ 40.2 MB/s |
| 6 GiB | fails at 63s | **stored**, 201s @ 30.5 MB/s |

Both stored with the right size and CRC32C. So OCI has no size ceiling anywhere
near s3's -- it accepted the very 6 GiB object s3 rejects outright. The limit is
purely `60s x throughput`: about 2 GiB on this link, and proportionally less on
a slower one. The largest object gsg can put therefore depends on the link
rather than on the file, which is why there is no number to document.

The error says "timeout", which reads like a transient network problem, so the
natural response is to retry -- and it fails again, every time, forever.

Both fail cleanly at least: exit 1, nothing stored, no partial object.

**And on oci the SDK's own retry is disabled by how gsg passes the body:**

```
Unable to perform Retry on this request body type,
which did not implement seek() interface
```

`Upload` passes `io.NopCloser(io.TeeReader(f, pb))` when a progress bar is
attached. The SDK reflects into `io.NopCloser` to find an `io.Seeker` so it can
rewind and retry, and a `TeeReader` is not one. Without a progress bar the body
is the `*os.File` and retries work, so this is only broken on the path the CLI
actually uses. Fixing it means passing something that reads, reports progress,
and still seeks -- a small wrapper over `*os.File` -- rather than a `TeeReader`.

Setting `ContentLength` explicitly, which #60 made come from the checksum pass,
does at least keep the SDK off its other path: without it the client measures
an unseekable body by reading the whole thing into memory first.

Note this is also why every gsg-written object is FULL_OBJECT and therefore
always has a comparable checksum -- the single-PUT path is what makes item 4's
fix work. Adding multipart would mean handling COMPOSITE checksums on gsg's own
uploads too, so the two interact.

**Plan, with every claim below prototyped against the real services.**

*The checksum objection is gone.* The worry recorded above -- that multipart
would make gsg's own objects carry a COMPOSITE checksum that `crc32cOf`
rejects, so rsync would re-copy them forever -- does not apply if the upload
asks for a whole-object checksum explicitly.

- s3: `CreateMultipartUpload` accepts `ChecksumType: FULL_OBJECT` alongside
  `ChecksumAlgorithm: CRC32C`, and `CompleteMultipartUpload` accepts the
  whole-file `ChecksumCRC32C` plus `MpuObjectSize`. Prototyped with a 2-part
  upload: `GetObjectAttributes` returned `ChecksumType=FULL_OBJECT` and a
  CRC32C byte-identical to the local whole-file value. `crc32cOf` stays exactly
  as strict as it is; no read-side change at all.
- oci: nothing to solve. Prototyped with 3 parts and a per-part
  `opc-content-crc32c`: commit returned the whole-file CRC32C and a later
  `HeadObject` still reported it, while `opc-multipart-md5` came back as the
  composite `...-3` form. Whole-object CRC32C and composite MD5, side by side.

*Do not use the SDK managers.* s3's `feature/s3/manager.Uploader` is not even a
dependency today and is composite-oriented; oci's `transfer.UploadManager`
buffers parts and never passes the CRC32C headers through. Exact checksum
control is the whole point, so hand-roll `CreateMultipartUpload` / `UploadPart`
/ `Complete`-or-`Commit` against the clients already in use.

*Read parts with `io.NewSectionReader(f, offset, length)`.* Nothing is
buffered, each part is independently seekable, and per-part retry falls out for
free -- which also repairs the seekability problem that stopped run 1 of the
40 GiB test from retrying at all.

**Measured: speed.** 2 GiB to oci, same file, same session.

| shape | throughput |
|---|---|
| single PutObject (today) | 36.0 MB/s |
| multipart, 64 MiB x 4 | 54.3 MB/s |
| multipart, 128 MiB x 8 | 48.1 MB/s |
| multipart, 256 MiB x 8 | 54.5 MB/s |

About 50% faster, and the full 3x3 matrix over 64/128/256 MiB and concurrency
4/8/16 ran between 39s and 46s -- all within noise of each other. **The gain
comes from having parallelism at all, not from tuning it**, so the part size
and concurrency defaults are not worth agonising over. The link saturates near
55 MB/s.

**Measured: a mid-transfer failure really is recoverable.** A failure injected
halfway through part 3 of 16:

```
part 3 attempt 1 failed: injected failure mid-part
parts=16 attempts=17 bytesSent=1.06x filesize crc ok
```

One retry, 6% of the file re-sent, correct whole-file CRC32C on the result.
Against the single PUT that failed at 454s of the 40 GiB run, which lost
roughly 17 GiB and had to start from zero.

**Fixed in PR #63 and #64.**

#63 removed the oci client's 60-second whole-request deadline, which had capped
an upload at roughly link speed x 60s rather than at any limit of the service.
#64 added multipart to both backends.

Measured after: 6 GiB to s3 in 48 parts at 46.2 MB/s, which returned
EntityTooLarge in 0s before; 300 MiB at 48.2 MB/s on s3 and 39.9 MB/s on oci;
and on oci a 2 GiB object went from 36.0 MB/s to 47-55 MB/s.

The checksum worry recorded above did not survive contact, and the fix is
verified end to end rather than by prototype: a build with ChecksumType
FULL_OBJECT removed stores a COMPOSITE checksum that crc32cOf rejects, and the
UAT assertion "a second rsync copies nothing, so the checksum is comparable"
then fails. With it, the assertion passes. oci never had the problem.

What was NOT done, deliberately: cross-process resume. Retry-in-run per part is
what landed, which costs one part rather than the whole transfer -- measured at
1.06x the file in bytes sent for one injected mid-part failure. A durable
manifest keyed on bucket, object, uploadID, path, size, mtime and part size is
a separate feature and easy to make dangerously stale.

Two judgement calls worth revisiting if they ever bite: the multipart threshold
is 128 MiB on both backends, and part concurrency is fixed at 8 rather than
following -c, because a recursive copy already runs one pool worker per file
and drawing part workers from the same pool could deadlock. Measured, nothing
is lost by the fixed value: 64, 128 and 256 MiB parts at concurrency 4, 8 and
16 all landed within noise of each other.

**Ordering, and what NOT to do first.**

1. `Pool`, `ChunkSize` and `GentleIO` do not currently reach the upload call
   sites -- `cmd/cp.go` and `cmd/rsync.go` pass only `Bars`. Fix that first, or
   part size and concurrency have nothing to read.
2. s3 multipart above 5 GiB, which is the only case that is impossible today.
3. oci multipart above a threshold, for the speed and the resumability.
4. **Abort on any failure, after in-flight parts settle.** Both services bill
   uploaded-but-uncommitted parts until the upload is completed or aborted, so
   a crash that leaks an upload costs money silently. A bucket lifecycle rule
   is worth recommending alongside.
5. Retry-in-run per part only. Cross-process resume needs a durable manifest
   keyed on bucket, object, uploadID, source path, size, mtime, part size and
   checksums; it is a separate feature and easy to make dangerously stale.

---

## 20. A command that fails on a backend error can exit in silence

The command layer discards the error it was given. `cmd/ls.go` is the clearest
case:

```go
if objs, err = fo.System.List(fo.Bucket, fo.Prefix, isRec); err != nil {
    common.Exit()
}
```

`common.Exit` is a bare `os.Exit(1)`, so nothing is printed. There are 21 sites
in `cmd/` that discard an error this way, and none of them log it first.

Today this is mostly hidden, because gs and s3 log inside the calls their `List`
makes -- so an error usually has been reported by the time it reaches here, by
something further down. It is luck rather than design: any backend error that
was not logged deeper produces exit 1 and an empty screen.

**Found while adding the OCI skeleton**, whose stubs return an error without
making any lower-level call that could log it. `gsg ls oci://bucket/` exited 1
and printed nothing at all. The skeleton works around it by logging inside
`errNotImplemented`, which is why that helper logs as well as returning.

**Fixed.** `common.ExitWith(err)` reports and then exits, and the command sites
that had an error in hand were converted to it.

It reports only when nothing else has, which the logger now tracks. The
backends log before returning, so reporting unconditionally would print every
ordinary failure twice -- the point is the case where nobody said anything at
all, not to say it again louder.

Two other things had to change for the message to be worth printing. The linux
backend wraps its shell commands, and exec reports only "exit status 1" -- the
tools write the real cause to stderr, which is now what gets returned. And the
inter-cloud copy paths named the source and destination but not the error.

Measured before: `gsg du` and `gsg cp -r` over a directory with an unreadable
subdirectory both exited 1 printing nothing. After: `cannot measure /tmp/p20:
du: /tmp/p20/noread: Permission denied`.

The original note follows.

**Fix:** log the error at the point it is discarded, or -- better, since it is
21 sites -- give `common.Exit` an error-taking form (`common.ExitWith(err)`)
that reports before exiting, and convert the sites to it. Worth doing before
the OCI backend is finished, so its real operations do not each need the same
workaround the stubs use.

---

## 21. Moving an object onto itself deletes it, on gs

`gsg mv` does not call `System.Move`. `cmd/mv.go` copies and then deletes the
source itself:

```go
doCopy(src, dst, true, isRec)
...
case system.FileType_Object:
    if err = src.System.Delete(src.Bucket, src.Prefix); err != nil {
```

So whether a self-move destroys the object depends entirely on whether that
backend's `Copy` fails when the source and destination are the same. Measured,
one object per backend, `gsg mv <path> <same path>`:

| backend | outcome |
|---|---|
| s3 | survives -- AWS rejects a copy onto itself, so the command exits before the delete |
| **gs** | **object is gone** |
| oci | survives -- `Copy` refuses a self-copy for exactly this reason |

The gs case is data loss from a plausible typo, and nothing warns. It is luck
rather than design that s3 escapes: AWS happens to reject the request, and the
`Delete` that follows is unconditional in both.

Found while building the OCI backend, where the same shape was reachable a
second way: `oci://b/k` and `oci://b@namespace/k` are one object with two
spellings, so a raw string comparison of source and destination misses it. That
one is fixed in the OCI backend.

**Fixed**, and it turned out to be two bugs rather than one.

Checking gsutil settled what the behaviour should be:

| | gsutil |
|---|---|
| `mv obj obj` | exit 1, "are the same file - abort" |
| `mv -r d d/sub` | performed; ends at `d/sub/d/...`, originals gone |

That pointed at the larger bug: mv listed the source *after* the copy, so for a
destination inside the source the listing returned the fresh copies too and
deleting them threw the data away. Measured on gs, `mv -r d d/sub` took two
objects to none. The delete list is now decided before the copy.

A self-descendant move is nonetheless refused, which is where gsg has to
diverge from gsutil. gsutil can perform it because its copy nests the source
directory -- it ends at `d/sub/d/...`, where nothing collides. gsg's `cp -r`
copies a directory's *contents*, so source and destination keys run into each
other: with `d/a.txt` holding "root" and `d/sub/a.txt` holding "nested",
`mv -r d d/sub` writes the first over the second and then deletes the second as
a source. Measured with the delete list already fixed, that still left one
object holding the wrong contents. Ordering the copies would not settle it
either, since they run in a pool.

`mv -r d d/` is refused for the same reason, one keystroke away.

The original note follows.

**Fix:** `cmd/mv.go` should not delete when the source and destination resolve
to the same object -- once, in the command, rather than relying on each
backend's `Copy` to fail. Comparing raw strings is not enough where a backend
has more than one spelling for a path. A guard in `GCS.Copy` would close the
measured case, but the command-level fix is the one that covers every backend.

---

## 22. IsDirectory lists the whole directory to answer a yes or no

`IsDirectory` asks whether anything exists under a path. Both backends answer
it by listing and counting.

`s3.IsDirectory` lists **recursively**:

```go
if objs, err = s.listObjectsAndSubPaths(bucket, prefix, true); err != nil {
```

so a prefix holding a million keys walks all million to return one boolean.
`gcs.IsDirectory` lists non-recursively, which is bounded by the number of
immediate children rather than everything beneath -- better, and correct, but
still a full page walk.

Measured against 1005 objects, warm clients, versus the same call on a
one-object directory:

| backend | 1 object | 1005 objects |
|---|---|---|
| gs | 75ms | 198ms |
| oci | -- | 17ms |

`FileType` calls this before nearly every command -- there are 27 call sites
across `cp`, `rm`, `ls`, `du`, `mv`, `rsync`, `lock` and `stat` -- so the cost
lands on all of them. A recursive copy pays it twice: once to decide the path
is a directory, then again to list it. The result is cached per `FileObject`,
so it is once per path rather than per object, but once per path at
O(objects beneath) is still the wrong shape.

**Fix:** ask for one entry rather than all of them. The OCI backend does this:
a single `ListObjects` with `Limit: 1` and a delimiter, so the service neither
returns a page nor walks the keyspace, and the work is constant regardless of
what is under the path. The equivalent is `MaxKeys: 1` with a delimiter on
`ListObjectsV2` for s3, and `Query{Delimiter: "/"}` with the iterator stopped
after the first item for gs.

Worth checking while there: a listing that carries only common prefixes and no
objects still has to count as a directory. Measured on OCI, `Limit: 1` does
return the prefixes, so a directory whose children are all sub-directories is
still recognised -- the same shape that truncated an s3 listing in item 2, so
it should not be assumed for `MaxKeys`.

Found while reviewing the OCI backend, whose first version copied the s3
shape.

## 23. A gs upload is not checked on arrival

`GCS.Upload` sets only the writer's metadata. It never sets `CRC32C` or
`SendCRC32C`, so no checksum is transmitted and GCS computes one from whatever
reached it.

That checksum is then what everything else compares against. An upload
corrupted in transit is stored with a checksum of the corrupted bytes, so
`rsync` sees a matching object and `-v` verifies the corruption against itself
and passes. Nothing anywhere reports a problem.

The mechanism exists and is unused. Measured against a real bucket:

| upload | outcome |
|---|---|
| no checksum sent -- what gsg does | succeeds; server computes CRC32C from what arrived |
| correct checksum sent | succeeds |
| wrong checksum sent | rejected: `Provided CRC32C "WG07Ig==" doesn't match calculated CRC32C "WG07IQ=="` |

Where the three backends now stand:

| backend | upload checked on arrival? |
|---|---|
| s3 | yes -- the aws sdk computes the checksum client-side and sends it, since #47 |
| **gs** | **no** |
| oci | yes -- gsg computes it and sends `opc-content-crc32c` |

**Fixed.** `GCS.Upload` now sends the checksum, so the service compares it
against the body that arrived and refuses the object if they differ.

The checksum is taken from the open handle rather than from the cache keyed on
path and mtime. The cache is cheaper -- and an earlier version of this fix used
it, guarded by `os.SameFile` -- but it is only as good as the assumption that
content and modification time move together. Where that is wrong the checksum
describes different bytes than the body does, and the service refuses the
object: a whole upload spent to be told it was stale. Measured on a 190MB file,
hashing the handle costs 111ms cold and 50ms warm against a 4s upload, so the
guess was never worth what it risked.

The original note follows.

**Fix:** two lines in `GCS.Upload`, before the first write --

```go
wc.CRC32C = common.GetFileCRC32C(srcFile)
wc.SendCRC32C = true
```

Both must be set, and both before the first `Write`: the library ignores
`SendCRC32C` afterwards, and zero is a valid checksum so it is not transmitted
on its own. Note `GetFileCRC32C` returns 0 both for a real zero and for a read
it could not complete (the other half of item 4), so a failed read would send a
zero and have a good upload rejected. That fails closed rather than storing bad
data, but it is worth fixing item 4 alongside, or computing the checksum here
in a form that can report failure.

Found while reviewing the OCI backend, which had the same gap.

**Fixed on gs in PR #57 and on oci in PR #60.** Both hash the open file handle
the body will be read from rather than reading the mtime-keyed cache, which
also settles the concern above about a failed read sending a zero: the checksum
comes from the same bytes that are sent, and a read error fails the upload
rather than being sent as 0. Measured at 111ms of hashing against a 4s upload
of the same 190MB file, so the cache was not buying much. PR #60 additionally
takes `ContentLength` from that same pass, so the declared length cannot
describe a different file than the body does.

## 24. A lock receipt identifies an object, not a holder

Releasing a lock needs proof it is ours: gs stores the generation, s3 and oci
store the ETag, in a /tmp file named from the bucket and object. One file per
object, so it records whoever locked it most recently on this machine rather
than any particular holder.

That is enough while a lock is held. It stops being enough once one expires:

| step | |
|---|---|
| A takes a lock with a short ttl | receipt = A's |
| the ttl passes | |
| B takes it over, same machine | receipt = **B's** |
| A runs unlock | reads **B's** receipt |
| | **B's lock is released** |

The conditional delete cannot help. The ETag it carries really is the current
holder's, so the service is right to honour it -- the wrong value was chosen
before the request was made.

**Measured on both s3 and oci**, same sequence, same outcome: `RELEASED-BY-A`.
gs stores a generation the same way and should be assumed to behave alike. Not
introduced by any recent change; it is how the receipt has always been keyed.

The window is exactly "a holder whose lock expired, then unlocks anyway", which
is what a long-running job does when it overruns its own ttl -- and the two
processes need not overlap, so a single-threaded machine is enough.

**Fix:** the receipt has to name a holder, not an object. Either a per-holder
receipt with the holder passing an identity to unlock, or a token in the lock
object itself that unlock reads back and compares before deleting. Both change
the interface, since `gsg unlock <url>` currently carries nothing to identify
who is asking, which is why this is filed rather than fixed alongside the OCI
backend.

Worth pairing with item 16, which is about the same filename being shared
between schemes.

Related, and cheaper: an ambiguous `PutObject` -- one the service committed but
whose response never arrived -- is read as a lost race on every backend. A
client-generated token in the lock body would let the caller check whether it
actually won, instead of assuming it did not. The lock is stranded until its
ttl either way, so this costs availability rather than correctness.

Found by review of the OCI locking backend; the uat there pins the current
behaviour so a fix is noticed rather than silent.
