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

## 25. A recursive move into a descendant escaped the guard when the bucket was spelled differently -- FIXED

`cmd/mv.go`'s `wouldDestroySource` refuses a move whose destination is the
source, or lives inside it. The second shape is the one that loses data: `cp -r`
writes a directory's *contents* into the destination, so `d` and `d/sub` both
want to produce `d/sub/a.txt`, and the delete that follows removes the source
list regardless.

It compared the buckets as written. oci accepts two spellings of one bucket, so

```
gsg mv -r oci://b@ap-singapore-1/d oci://b@axkm4tp1h2ba.ap-singapore-1/d/sub
```

passed it: two different strings, so the guard returned false. The backend's own
self-copy check does not catch this either -- `sameObject` compares one object
to one object, and this is a directory to its own descendant, which is the case
this guard exists for. The copy wrote into the tree being moved, then the delete
ran over a source list computed before it.

**Fixed** by comparing what the backend resolves rather than what was typed,
in an order that keeps it cheap and safe. The prefixes are settled first,
because they are free and they decide whether the buckets matter at all: where
the destination is neither the source nor inside it, no pair of buckets can
make the move destructive. Only when the prefixes do collide, and only when the
two paths are spelled differently, is the backend asked to resolve them --
through an optional interface:

```go
type bucketCanonicaliser interface {
    CanonicalBucket(spec string) (string, error)
}
```

Only oci implements it -- gs and s3 give one bucket one spelling, so they are
never asked and `ISystem` did not change.

A backend that says it can resolve and then cannot -- no credentials, a
namespace lookup that failed for a moment -- makes the guard **refuse**.
Falling back to the raw strings there, which the first version of this fix did,
reproduces the original bug exactly whenever resolution fails for an instant:
the two spellings compare unequal, the guard waves the move through, and the
copy and delete that follow resolve the namespace perfectly well on their next
attempt. A move refused because gsg could not tell costs a retry; a move
allowed because gsg could not tell costs an object. Because the prefixes are
checked first, this can only ever refuse a move that was already the dangerous
shape.

The first draft of this entry claimed the fix needed a new method on `ISystem`
that every backend would have to implement, and filed it on that basis. That
was wrong: an optional interface reaches only the backend that needs it.

Verified against a real bucket both ways. With the guard reverted to comparing
the paths as written, `mv -r oci://b@ap-singapore-1/rec
oci://b@<ns>.ap-singapore-1/rec/sub` over `rec/a.txt` ("root") and
`rec/sub/a.txt` ("nested") exited 0 and left a single object, `rec/sub/sub/a.txt`
-- two objects in, one out. With the fix it is refused, both objects survive,
and a genuine move using the same alternate spelling to a key outside the tree
still moves.

Covered by `TestWouldDestroySourceSeesThroughBucketSpellings`,
`TestWouldDestroySourceRefusesWhenItCannotTell`,
`TestCanonicalBucketIsAskedOnlyWhenItCanChangeTheAnswer`, their counterpart
that a same-named bucket in another region is *not* collapsed -- which matters
just as much, since collapsing them would refuse a legitimate cross-region
move -- and in the uat by the recursive descendant move above.

## 26. A cross-region copy has never run against the service

`oci://bucket@region/key` lets one process address several regions, and `Copy`
sends the destination's own region as `CopyObject`'s `DestinationRegion`, so a
copy between regions should work. Nothing has ever proved that against a live
service.

The obstacle is a bucket, not the code. The uat tenancy is subscribed to
`ap-singapore-1` (home) and `us-phoenix-1`, but every bucket it has is in
Singapore, so `uat/oci/55-cross-region.sh` skips. That file is written and
waiting: set `GSG_UAT_OCI_BUCKET2` and `GSG_UAT_OCI_REGION2` and it runs four
cases -- two regions addressed from one command, a copy across them, a move
across them, and a round trip back to a file.

What *is* established, so the gap is narrower than it sounds:

  - **Routing.** A request for a bucket in `us-phoenix-1` demonstrably reaches
    Phoenix: it resolves the namespace there and returns `BucketNotFound` with
    a `phx-1:` request id. A second region's client really is built and used.
  - **Addressing.** `TestCopyRequestNamesTheDestinationsRegionNotTheSources`
    pins that `DestinationRegion` carries the destination's region, and fails
    when it is reverted to the source's. That is the field the bug was in.
  - **Everything either side of it.** The copy path itself is covered between
    two buckets in one region by `uat/oci/52-cross-bucket.sh`.

So the untested claim is specifically that the service honours a
`DestinationRegion` naming a region other than the one the request was sent to
-- including whether the work request, which `awaitWorkRequest` follows through
the *source's* client, reports completion the same way when the object lands
elsewhere. That last part is the one worth watching: if a cross-region work
request is tracked in the destination's region instead, the poll would fail to
find it and the copy would report an error after having succeeded. For `Move`,
which deletes only after the copy is confirmed, that fails safe -- the source
survives and nothing is lost -- but it would still be wrong.

**To close it:** create a bucket in a second subscribed region, then

```
GSG_UAT_OCI_BUCKET2=<bucket> GSG_UAT_OCI_REGION2=us-phoenix-1 ./uat.sh oci
```

Filed when the region-in-the-path change landed, with the cross-region cases
written but skipped for want of a second bucket.

---

## 27. An upload reads the whole file an extra time to checksum it

`gcs` and `oci` both compute the whole-object CRC32C in a pass of its own,
before the pass that sends the bytes. For `oci` above the multipart threshold
there is a third pass, because each part is read once to checksum it and again
to send it:

```go
// oci/multipart.go
wholeCRC, _, err := crc32cOfReader(f)                       // 54  -- whole file
...
io.Copy(ph, io.NewSectionReader(f, off, length))            // 124 -- part, to a hasher
UploadPartBody: io.NopCloser(io.NewSectionReader(f, off, length)),  // 134 -- part, to the wire
```

```go
// gcs/gcs.go
crc, cerr := crc32cToSend(f, srcFile)                       // 737 -- whole file
...
io.Copy(io.MultiWriter(wc, pb), io.NewSectionReader(f, off, length))  // 822 -- part, to the wire
```

So a multipart upload reads the file three times on `oci` and twice on `gcs`.
The two per-part reads on `oci` are deliberate and worth keeping -- the comment
at lines 119-122 says why, and it is a good reason: a `SectionReader` per part means
nothing is buffered and each part stays independently seekable, so the SDK can
rewind and retry one part instead of the whole transfer. The second read is
also usually cache-warm, since it follows the first immediately.

The *first* pass is the one worth removing. It runs to completion before any
part is sent, so by the time the parts are read its pages have been evicted:
it is a genuine cold read of the whole file, and its only purpose is to know
the value the service is later asked to confirm.

### What it costs

The pass is unpaced -- a plain `io.Copy` in both backends -- and neither
`crc32cOfReader` nor `crc32cToSend` goes through `common.GetFileCRC32C`, so
`common.GentleIO` does not reach it and neither does its cache. `oci` never
reads `RunContext.GentleIO` at all, so on that backend nothing about an upload
is paced.

Measured downstream: jam-core uploads a 33GB `links.csv` and a 5.7GB
`junctions.csv` at bake time, on the same pod whose BoltDB mmap it applies
`MADV_RANDOM` to protect. For `links.csv` alone that is ~99GB of reads through
the page cache where ~66GB would do; across both files, ~116GB against ~77GB.
None of it is paced.

What makes that a regression for jam-core rather than merely a cost is what it
is replacing. jam-core does not use this repo for GCS -- it has its own client,
whose upload is a single `io.Copy` under `FadviseSequential` with
`FadviseDontNeed` after. So the same 33GB upload goes from one advised read to
three unadvised ones when the bucket moves from `gs://` to `oci://`. Within
this repo the comparison is narrower: `gcs` does two unadvised reads, `oci`
three, and this item is the one read both have to spare.

### What a fix would involve

The primitive is already here. `common.CombineCRC32C` (#68) returns the CRC32C
of A followed by B from the two sums and B's length, so the parts can be summed
independently, in any order, and folded afterwards -- which is exactly what
`oci/multipart.go` already computes per part at line 124 and then throws away
once the part is committed.

On `oci` the sums already exist and are discarded: keep each part's raw
`uint32` beside its `PartNum` in the `commit` slice, which is already indexed
by part number, fold them in order once every part is done, and drop
`crc32cOfReader` from `uploadMultipart`.

`gcs` needs one more step first, because its composite path takes no local sum
at all -- line 822 copies to `wc` and `pb` and nothing else. The sum has to be
computed **in that same copy**, by adding a hasher to the `MultiWriter`. Folding
the checksums GCS returns for the parts would not do: those describe whatever
reached the service, so in-transit corruption would agree with itself, which is
the property #47 and #57 exist to provide. Taking the sum in a separate pass
would not do either -- that is the read this item is about. Once the local sums
are taken alongside the write, `crc32cToSend` can go from the composite branch.

Both functions stay for the single-request paths, which genuinely need the
value before they start.

`gcs`'s download side already works this way, and for the same reason --
`verifyGentleDownload` settles the transfer from the sums its chunks took while
writing rather than reading the file back, because "gentle mode has been asking
the kernel to drop it from the page cache all along, so that read would come
from disk, as large as the file, against whatever else is reading that disk"
(#70). That argument applies unchanged to the upload side.

A second property comes free, and it removes code rather than adding it. The
whole-file sum and the part sums are taken at different times, so a file
rewritten in between produces parts that each validate against a whole-object
sum that no longer describes them -- a hazard `uploadMultipart` already
documents at lines 181-183 and has to compensate for. The compensation is
expensive: the mismatch is only detectable *after* `CommitMultipartUpload` has
published the object, so the backend deletes it afterwards (lines 191-200), and
if that delete fails the wrong object stays visible where callers will read it.

Folding the part sums makes the object's checksum and its bytes the same bytes
by construction, so that window closes.

**The response check and the deletion still have to stay.** They cover more
than source mutation: `CommitMultipartUpload` can return no checksum at all, or
one that disagrees because the assembly or the response itself was faulty, and
the object is published before either can be seen. What folding removes is one
cause of a mismatch, not the need to handle one.

**Related:** `oci` ignoring `RunContext.GentleIO` is a separate, smaller fix --
`gcs/gcs.go:442` and `s3/s3.go:784` have the pattern to copy. It compounds this
one, since the passes that remain after this item are unpaced too.

Filed from jam-core's oci integration (nextbillion-ai/jam-core#104), where the
upload path was the one place gentle I/O could not be preserved.

**Fixed on oci in PR #76. `gcs` still reads its file twice** and this item
stays open for it -- deliberately, because `gcs`'s half needs the hasher added
to the composite path's `MultiWriter` at `gcs/gcs.go:763`, which is the same
line item 28's upload half would touch. On `oci` the order is the other way
round: 27 first, because pacing a pass that is about to be deleted makes the
doomed pass slower and pollutes any measurement of what the pacing costs.

`oci` now keeps each part's sum beside its length and folds them with
`common.CombineCRC32C` once every part is done, so three passes over the file
became two. `crc32cOfReader` stays for the single-request path, which genuinely
needs the value before it starts.

**The predicted free property arrived, and took a real check with it.** The
whole-file sum and the part sums were taken at different times, so a file
rewritten in between produced parts that each validated on arrival while only
the whole-object sum disagreed -- detectable only *after*
`CommitMultipartUpload` had published the object, which is why the backend
deletes it afterwards. Folded, the checksum and the bytes are the same bytes by
construction, so that cannot happen.

But that pass was also the only thing that *noticed* a source rewritten
mid-upload. Without it every part validates, and the object is published
holding a mix of two files with a checksum matching the mixture. So the file's
size and modification time are now compared either side of the parts, and the
upload is refused when they moved -- before the commit, so nothing is published
and the deferred abort is the whole of the cleanup, which is strictly better
than the delete-after-publish it replaces.

Weaker than the hash, and recorded as such: a rewrite preserving both size and
mtime is invisible to it. That is the assumption the crc32c cache makes and
that #57 and #60 refused to make about the bytes being *sent*; here it only
decides whether to distrust an upload that has already verified itself part by
part.

Review found the gap that made this dangerous rather than merely weaker. The
part geometry was computed from one stat in `Upload` and the baseline taken
from a second one inside `uploadMultipart`, so a file appended to between them
left both the baseline and the check describing the larger file while the parts
still stopped at the old size -- a silently truncated object, committed
successfully. The pass this change removed used to read the appended bytes and
catch it. `Upload`'s stat is now passed in, so there is no second stat to
disagree: the gap is closed by construction rather than checked for.

**The post-commit response check and the delete stay,** as this entry said they
must. What folding removes is one cause of a mismatch, not the need to handle
one: a commit can report no checksum at all, or one that disagrees because the
assembly or the response was faulty.

**Measured: nothing, and that is the honest answer on this link.** Three
interleaved 2 GiB uploads per binary spread 38.9-51.2 MB/s with the two fully
overlapping. The upload is network-bound and the source was in page cache, so a
laptop on a 40 MB/s link cannot show a read that is no longer there. The win is
read amplification where the source does not fit in cache -- jam-core's 33GB
`links.csv` goes from ~99GB of reads to ~66GB -- which is arithmetic, not
something this measurement can confirm.

Covered by `TestThePartSumsFoldToTheChecksumOfTheWholeFile`, which holds the
fold against the whole file hashed in one go rather than against a second copy
of the same arithmetic, and by `TestSourceMovedSeesASourceRewrittenUnderTheParts`,
whose last assertion pins the gap rather than implying it. In the uat the
multipart upload's exit code is now asserted -- the cheapest real check on the
folding, since a wrong fold fails the commit outright -- and a new case rewrites
a file under its own parts and requires the upload to be refused with nothing
stored.

### What actually happens to a file changed mid-upload

Traced against the bucket afterwards, since the entry above reasons about it
and reasoning is not evidence. A 200 MiB upload, mutated two seconds in:

| the source is | outcome | what catches it |
|---|---|---|
| truncated to half | refused, nothing stored | the transport: `ContentLength=134217728 with Body length 104857600` |
| appended to | refused, nothing stored | the size and mtime comparison, naming both sizes and times |
| rewritten in place, mtime put back | refused, nothing stored | **the service**: 400 InvalidContentChecksum on a part |

The third is the one worth understanding, because it is the case the size and
mtime comparison is blind to by construction -- and it was still refused. Each
part is read twice, once to checksum it and once to send it, and a rewrite that
lands between those two reads for any part in flight makes that part fail its
own `opc-content-crc32c`. With eight parts in flight there is a lot of window
to land in.

So the documented gap is narrower than "same size and mtime defeats it": the
rewrite also has to miss every in-flight part's hash-to-send interval. That is
possible -- a rewrite entirely between parts, with the mtime restored -- and
would store an object holding a mix of two files. It was not reproduced, and is
recorded as the residual rather than as something demonstrated.

Tracing this also found a real gap and closed it: `io.Copy`'s byte count was
discarded, so a section reader stopping early -- which is what a truncated file
gives, with no error -- still recorded the length the part was *planned* for,
and that length folds into the whole-object checksum. It would have described
an object nobody uploaded. The count is now checked. It is not what usually
reports a truncation, as the table shows, and the comment on it says so.

No multipart upload was left dangling by any of the three: the deferred abort
runs on every path out.

---

## 28. The oci backend ignores gentle I/O

`RunContext.GentleIO` reaches every backend and only two read it:

```
grep -rn GentleIO gcs/ s3/ oci/
  gcs/gcs.go:442    if ctx.GentleIO {
  s3/s3.go:784      if ctx.GentleIO {
  oci/              (nothing)
```

So `gsg cp --gentle-io` from `oci://` is accepted, reports nothing unusual, and
paces nothing. `oci/transfer.go:92` writes the body with a plain `io.Copy` into
a 4MB `bufio.Writer`, and the upload paths read the file through `io.Copy` as
well (see 27).

The one lever that does reach `oci` is the `common.GentleIO` package global,
because `common.GetFileCRC32C` consults it -- but that only covers checksum
reads a caller makes itself, not any transfer this backend performs.

### What it costs

Gentle mode exists so that moving a large object does not evict everything else
from the page cache. jam-core is the case it was built for: it downloads
prebaked BoltDB crates of 80GB and uploads a 33GB `links.csv` on the same pod
whose BoltDB mmap it applies `MADV_RANDOM` to protect, and it sets
`GENTLE_IO` for exactly that reason. On `gs://` it gets what it asked for. On
`oci://` the flag is silently inert, which is worse than not offering it --
the caller believes the transfer is paced.

### What a fix would involve

Follow `gcs`, and only `gcs`. It wraps the per-chunk write with
`common.FadviseWriteSequential` and a throttled 1MB copy loop, sums each window
in flight, and asks the kernel to drop it once written (`adviseRange` +
`common.FadviseWriteDontNeed`, `gcs/gcs.go:473`). The trigger is a watermark:

```go
if totalWritten-summed >= gentleWindow {   // gcs/gcs.go:491
```

**`s3` is not a smaller working version of that -- its throttle does not
reliably run at all**, and copying it would ship a gentle mode that silently
does nothing. It has the fadvise calls (`s3/s3.go:806` and `820`), but fires
the in-loop one on an exact modulo boundary:

```go
if totalWritten%(10*1024*1024) == 0 {      // s3/s3.go:805
```

`Read` into the 1MB buffer at `s3/s3.go:789` returns whatever the socket has,
not a full buffer, so `totalWritten` advances by arbitrary amounts and can step
over every exact multiple of 10MiB without ever landing on one. When it does,
neither the drop nor the 20ms pause happens during the transfer, and only the
final `FadviseWriteDontNeed` after the loop runs -- which is the whole file at
once, the opposite of pacing. `gcs`'s `>=` watermark cannot be stepped over.
That is a defect in `s3` in its own right and deserves its own entry.

**The part not to miss.** `oci.Download` ends with

```go
return o.MustEqualCRC32C(forceChecksum, dstFile, bucket, prefix)
```

and that reads the whole file back (`oci/checksum.go:35`,
`common.GetFileCRC32C`). Under gentle mode those pages have just been dropped
on purpose, so the verification would come straight off the disk, as large as
the file, against whatever else is reading that disk -- the exact defect #70
removed from `gcs`. `gcs` now sums each window from the file at its offset
while the pages are still cached and immediately before dropping them
(`gcs/gcs.go:468`), then settles the transfer from those sums
(`verifyGentleDownload`). `oci` needs the same shape, not merely a throttled
write; `s3` does not have it yet either.

**Uploads need their own answer, and there is nothing to copy.** Both cited
branches are download code -- `GentleIO` is read nowhere else in either backend
-- so following `gcs` and `s3` fixes only half of this, and it is the half that
does not cover the 33GB `links.csv` above. On the upload side `oci` would be
leading rather than following: the reads to advise and throttle are
`crc32cOfReader` and the part bodies in `oci/multipart.go`, and the single-PUT
path in `oci/transfer.go`. 27 reduces how many of those reads there are; this
item is what paces the ones that remain. Neither subsumes the other.

This overlaps 29. The chunk loop that item adds is where the download pacing
belongs, so those two are cheaper done together than apart.

**Fixed on the download side in PR #74,** which is the half that shares its
loop with 29. The upload half is not done and this item stays open for it.

`oci.Download` now paces exactly the way `gcs` does, because it is now the same
code: the window loop, the fadvise calls and the in-flight sums moved to
`common.GentleWrite`, and `gcs` calls that rather than its own copy. Two
backends reading `GentleIO` became three, and a correction to any of it lands
in one place instead of two -- which is what item 13 is about, and this loop is
subtler than the atomic write that item concerns.

The part not to miss was the verification, and it was there: `oci.Download`
ended at `MustEqualCRC32C`, which reads the whole file back. Under gentle mode
those pages have just been dropped on purpose, so that read comes off the disk,
as large as the file, against whatever the pacing was protecting. It now folds
the per-chunk sums `GentleWrite` took while writing -- `common.FoldCRC32C`,
over `common.CombineCRC32C` (#68) -- and compares that, so a gentle download
reads the file exactly once.

Verified against the bucket rather than argued: `uat/oci/45-download.sh` runs a
20 MiB object of random bytes through `--gentle-io -v` in twenty chunks and
requires both `cmp` and the checksum to agree. Those two catch different
things. A fold that got the order or the lengths wrong gives a wrong checksum
on a file `cmp` is perfectly happy with, so it is `-v` passing on a file that
also compares equal that says the fold is right. The empty object and the
library caller's shape -- no pool, no progress bar -- go through the same case.

Measured, gentle mode costs nothing worth reporting: six interleaved runs of
the same 1 GiB object landed between 54 and 66 MB/s with gentle and plain on
both ends of that spread, so the pacing is below the noise of the link. What it
buys cannot be measured on macOS at all, where the fadvise calls are no-ops;
the eviction is real only on linux, which is true of `gcs`'s gentle mode too.

Review caught a defect in the pacing that `gcs` has had all along, and that
this change would have made much worse. A drop request on dirty pages only
starts their writeback, so a window can go no sooner than the next request
covering it -- which every window got from the window after it, except the
last. And the pause fired only on a *full* window. Per whole-file chunk in
`gcs` that left a tail; per `--chunk-size` piece here it would have left
everything, since a chunk of 1 MiB never completes a 10 MiB window: gentle mode
present and doing nothing, which is exactly item 30. Each call now pauses for
the remainder it did not pace, in proportion -- so the rate no longer depends on
how the caller cut the transfer up -- and then makes one more drop request over
everything it wrote. The pause comes first on purpose: two requests in
immediate succession can both find the same pages still dirty, and then neither
drops anything. Waiting for the writeback outright means `sync_file_range`,
which is linux-only and not what `common` does today, so on a busy enough disk
the tail can still survive. `gcs` gets all of it by sharing the code.

Neither the sleep nor the advice is observable -- the advice is a no-op off
linux, a sleep leaves no trace -- which is how it survived review twice. Both
are swappable now, and `common/gentle_test.go` asserts the rate and that every
byte written is asked for at least twice.

**Still open: the upload half.** `GentleIO` is read by the download path on all
three backends now and by no upload path on any of them, so the 33GB
`links.csv` above is still unpaced -- and item 27's first pass, the one that
reads the whole file just to checksum it, is still unpaced too. That work has
nothing to follow and is unchanged by this.

**The s3 defect this entry describes is now item 30,** as it said it deserved.

---

## 29. An oci download is a single unranged stream

`gcs` and `s3` both split a download into chunks, fetch each with a ranged GET,
and run them through the shared worker pool:

```go
// gcs/gcs.go
ctx.Pool.AddWithDepth(1, ...)                                    // 405
rc, err := g.client.Bucket(bucket).Object(prefix).NewRangeReader( // 422

// s3/s3.go
ctx.Pool.AddWithDepth(1, ...)                                    // 746
Range: aws.String(fmt.Sprintf("bytes=%d-%d", startByte, startByte+length)),  // 762
```

Copy the shape, not that last expression. RFC 7233 range endpoints are
inclusive, so `startByte+length` asks for `length+1` bytes: neighbouring chunks
overlap by one, and the final chunk asks for one byte past the end. Nothing is
corrupted -- each worker seeks to its own `startByte`, so the shared byte is
written twice with the same value, and the service clamps the overshoot -- but
every chunk transfers a byte it does not need. The correct endpoint is
`startByte+length-1`. Worth fixing in `s3` separately; noted here so it is not
propagated.

A zero-byte object needs handling before the range is built at all. The chunk
geometry floors the count at one, so an empty object still produces a single
chunk of length 0, and the corrected endpoint yields `bytes=0--1` -- not a
range. Serve size 0 without a ranged GET.

`oci/transfer.go` does neither. One `GetObject` with no `Range`, one `io.Copy`,
no chunking and no pool. `GetObjectReader` has no offset or length parameter
either, so a library caller cannot assemble the behaviour from outside:

```go
func (o *OCI) GetObjectReader(bucket, prefix string) (io.ReadCloser, error)
```

The service is not the obstacle. The OCI SDK carries
`GetObjectRequest.Range` (RFC 7233), with `ContentRange` on the response.

### What it costs

One stream is one TCP connection and one congestion window, so throughput is
capped by that connection however much bandwidth the host has. #68 measured the
same ceiling from the other direction on `gs`: multiplexing every request onto
one HTTP/2 connection capped downloads near 235 MB/s, and spreading them over
pooled HTTP/1.1 connections is what lifted it. An 80GB prebaked crate, which is
what jam-core fetches per map release, is fetched on 16 connections from `gs://`
and on one from `oci://`.

The second cost is that per-chunk recovery is not even expressible. A
single-stream download that fails at 79GB starts again at zero. Chunking makes
losing only one chunk *possible*, but it does not deliver it on its own, and
neither model backend implements it: `gcs` and `s3` both call `common.Exit()`
when a chunk fails, so today the whole command dies either way. The SDK retries
getting the response, not a mid-body read -- once `Content` is returned it is a
plain `io.ReadCloser`. A chunk loop worth having needs an explicit retry around
each chunk, which is a small addition once the loop exists and is the reason to
count this as a benefit rather than assume it.

### What a fix would involve

Mirror the `gcs` loop: size the chunks from `ctx.ChunkSize`, pre-allocate the
destination, submit each chunk to `ctx.Pool`, fetch it by setting `Range` on
`GetObjectRequest`, and write it at its offset. Add a ranged variant of
`GetObjectReader` alongside the existing one so the capability is also
available to library callers.

Pin the version every chunk reads. Independent ranged GETs by object name can
land on either side of an overwrite, and the pieces would assemble into a file
that never existed -- returned silently when `forceChecksum` is off. The HEAD
that `Download` already performs gives the value to pin with: pass its ETag as
`IfMatch` on every `GetObjectRequest`, or its `VersionId`, and fail the
transfer when it stops matching. Both fields exist on that request. `gcs` and
`s3` do not do this today -- their range reads are by name -- so this is one
place to improve on the model rather than follow it.

Pinning has to reach the verification too, or it introduces a failure of its
own. `Download` currently ends at `MustEqualCRC32C`, which HEADs the key afresh
by name; an overwrite landing after the last chunk but before that call would
compare a correctly assembled copy of the pinned version against the
replacement's checksum and report the transfer as corrupt. Verify against the
checksum the first HEAD returned, not a second lookup.

Two things specific to this backend:

  - **`oci` does not use the shared pool at all today.** Its multipart upload
    runs parallel parts through a semaphore of its own
    (`oci/multipart.go:94`, `common.PartConcurrency`), so `-m` sizes the pool
    that `gcs` and `s3` transfers share while `oci` concurrency is decided
    separately and ignores it. Worth resolving in the same change rather than
    adding a second independent knob.
  - **`ctx.Pool` may be nil.** It is always set by `cmd/`, but a library caller
    need not: jam-core passes `system.RunContext{ChunkSize: ...}` and nothing
    else, which is safe today only because `oci` never touches the pool. If the
    chunk loop calls `ctx.Pool.AddWithDepth` unguarded, that caller gets a nil
    dereference on the first chunk of its first download. Either run inline
    when the pool is nil, or make the field's requirement explicit.

Doing this alongside 28 is what makes both worth having: the chunk loop is
where the pacing lives in `gcs`, and once `oci` has both, jam-core can delete
the download it hand-wrote precisely because this backend had neither
(nextbillion-ai/jam-core#104, `pkg/cloud/oci_object_storage.go`).

**Fixed in PR #74.**

`oci.Download` fetches the object as parallel ranged chunks: geometry from
`ctx.ChunkSize` with the same reading of the flag the other two give it
(negative is the 16 MiB default, zero means do not chunk), one `GetObject` per
chunk with `Range`, written at its own offset into the pre-allocated temporary
file. `GetObjectRangeReader` offers the same to a library caller, which could
not assemble it from outside before.

Three things were done differently from the backends it follows, each because
following them would have shipped a known defect:

  - **The range endpoints are inclusive.** `s3` asks for
    `bytes=startByte-(startByte+length)`, so every chunk there fetches one byte
    more than it needs. That is now item 31. Here each chunk also checks what
    it was given against what it asked for, so an overshoot -- or a service
    that ignored the range and answered with the whole object, which would
    otherwise have each chunk write the whole object at its own offset -- is an
    error rather than something that assembles quietly.
  - **Every chunk is pinned.** `IfMatch` carries the ETag the first HEAD saw,
    so independent ranged GETs cannot land either side of an overwrite and
    assemble a file that never existed. `gcs` and `s3` read by name and can.
    The verification is pinned to that same HEAD's checksum rather than a fresh
    lookup, or an overwrite landing after the last chunk would have a correctly
    assembled copy of the pinned version reported as corrupt.
  - **A failed chunk is retried, not fatal.** `gcs` and `s3` call
    `common.Exit()` on a chunk failure, so a blip 79 GiB into an 80 GiB object
    costs the whole transfer. Per-chunk `common.DoWithRetrySimple` costs one
    chunk. The progress bar has to be wound back by hand when an attempt is
    abandoned, or a retried chunk counts its bytes twice.

An empty object is fetched without a range at all: the geometry floors at one
chunk, so it would otherwise ask for `bytes=0--1`, which is not a range.

**Measured, and less than the entry above assumed.** A 1 GiB object from
ap-singapore-1, same session, `--chunk-size 0` being the single stream this
replaced:

| shape | |
|---|---|
| single stream | 16.4s, 17.5s -- 62.5, 58.5 MB/s |
| chunked, 16 MiB, `-m` | 15.2s, 15.2s -- 67.5, 67.2 MB/s |
| chunked, no `-m` | 17.3s, 17.2s -- 59.2, 59.5 MB/s |

About 10%, not the multiple #68 saw on gs, and the reason is visible in the
numbers: one connection already reaches 58-62 MB/s on this link and everything
in flight together reaches 67, so there is almost no headroom to take. Shape
makes no difference within that -- 4 MiB x 256, 64 MiB x 16 and `-c 8` all
landed between 63.7 and 65.3 MB/s. The case for this is therefore the
recoverability and the pacing, not the throughput, on *this* link; the
throughput argument is for a host whose bandwidth one connection cannot fill,
which is the case jam-core fetches its 80GB crates on.

Note the third row. Chunks go to the shared pool, so without `-m` the pool has
one worker and the chunks run one after another -- marginally slower than the
single stream it replaced, since it is the same bytes in more requests. That is
what `gcs` and `s3` do too.

**`ctx.Pool` being nil is handled by running the chunks under a local bound
instead.** The alternative the entry above offered -- run them inline -- would
have left the one caller this change is for, which passes `ChunkSize` and
nothing else, with a serial download and no benefit at all.
`uat/oci/45-download.sh` drives `Download` from a Go program with that exact
RunContext, plain and gentle, because the cli always supplies a pool and a
progress bar and so cannot reach either absence. Both are hazards rather than
defaults: a nil pool dereferences, and a nil `*bar.ProgressBar` passed as an
`io.Writer` is not a nil `io.Writer`, so writing to it dereferences the nil
receiver inside `IncrBy`.

**What was NOT done.** The upload's part concurrency still comes from
`common.PartConcurrency` rather than the pool. Moving it would make a default
upload -- no `-m`, one pool worker -- send its parts one at a time, where today
it sends eight, so the second bullet above resolves the other way than that
note assumed: two knobs, and the reason for each written down, rather than one
knob that costs the default case its parallelism.

---

## 30. The s3 gentle throttle fires on a boundary it can step over

`s3.Download` has the fadvise calls gentle mode is made of, but reaches the one
inside the loop only on an exact multiple of 10 MiB:

```go
if totalWritten%(10*1024*1024) == 0 {      // s3/s3.go:805
```

`Read` into the 1 MiB buffer at `s3/s3.go:789` returns whatever the socket has,
not a full buffer, so `totalWritten` advances by arbitrary amounts and can step
over every exact multiple without ever landing on one. When it does, neither
the drop nor the 20ms pause happens at all during the transfer, and the only
fadvise that runs is the one after the loop -- the whole file at once, which is
the opposite of pacing.

So `gsg cp --gentle-io` from `s3://` may pace nothing, depending on how the
socket happens to deliver. It is not inert the way `oci` was before item 28,
which would at least be predictable; it is a coin toss.

`gcs` and `oci` use a `>=` watermark, which cannot be stepped over:

```go
if written-summed >= GentleWindow {        // common/gentle.go
```

**Fix:** `s3` should call `common.GentleWrite`, which is what `gcs` and `oci`
already do -- the window loop, the fadvise calls and the in-flight sums live
there since PR #74. It is close to a deletion: the s3 branch becomes the
same half-dozen lines the other two have.

That also settles the other half, which is the same defect item 28 found on
`oci`: `s3.Download` ends at `MustEqualCRC32C`, reading the whole file back
after gentle mode has spent the transfer dropping it -- so the verification
comes off the disk, as large as the file. `GentleWrite` returns the sums to
settle it from instead.

Found while giving `oci` a gentle download, by reading `s3` as a second model
and finding it was not one.

## 31. An s3 ranged GET asks for one byte too many

`s3.Download` builds each chunk's range as

```go
Range: aws.String(fmt.Sprintf("bytes=%d-%d", startByte, startByte+length)),  // s3/s3.go:762
```

RFC 7233 range endpoints are inclusive, so that asks for `length+1` bytes:
neighbouring chunks overlap by one and the final chunk asks for one byte past
the end of the object. The correct endpoint is `startByte+length-1`.

Nothing is corrupted, which is why it has gone unnoticed -- each worker seeks
to its own `startByte`, so the shared byte is written twice with the same
value, and the service clamps the overshoot. It costs one extra byte per chunk
on the wire, and it is wrong in a way that would matter the moment anything
downstream started trusting the byte count.

**Fix:** subtract the one. `oci` does, and checks the byte count it got against
the count it asked for, so the mistake cannot be made there without a test
failing -- see `TestRangeHeaderEndpointsAreInclusive` and the case in
`uat/oci/45-download.sh`.

Noted while writing the `oci` chunk loop, from the model it was copied from.

---

Items 32 to 42 were found on 2026-09-19 by testing main at fbdc019 against
`gs://gsg-uat`: `uat.sh gs` under the race detector (156 assertions, all
passing), then supplementary cases for what it does not reach, some of them
through `uat/faultproxy` and some repeated under Linux in the pipeline's
`xsmtools` image, because the libraries retry differently there.

Most items end with a case written for `uat.sh`: it runs in `do_test` from
`uat_temp` with `../gsg`, `$remote_base` and the helpers already there, and it
is meant to go in with the fix. Each was run against fbdc019: it fails there
only on assertions about the item's own defect, and the assertions that guard
what must keep working already pass (item 39's case passes except for its last
assertion, which is item 40). Item 42 has no case. The fault-injection cases
use the `fp_*` helpers `uat.sh` defines, which start `uat/faultproxy`
themselves. Items 32, 33 and 35 are fixed, and their cases have moved into
`uat.sh`.

## 32. A gs download can assemble a file from two versions of an object

`gcs.Download` takes the size from one lookup (`gcs/gcs.go:360`) and then opens
every chunk by name:

```go
rc, err := g.client.Bucket(bucket).Object(prefix).NewRangeReader(   // gcs/gcs.go:422
```

Nothing ties the chunks to the generation that lookup saw. A chunk opened after
the object is replaced reads the new generation, and the pieces are written
into one file. Without `-v` nothing checks the result: `MustEqualCRC32C` returns
at once when `forceChecksum` is off, and the gentle path's `verifyGentleDownload`
logs the mismatch and returns nil. Within one chunk it is safe: the storage
reader pins the generation of its first response when it reopens.

Measured: a 64 MiB object fetched with `--chunk-size 1048576` and no `-m`, so
the chunks are read one after another, and replaced by a server-side copy 1.5 s
in. Twice in a row `gsg cp` exited 0 with a file holding 7 MiB of the old
version and 57 of the new, then 20 and 44. With `-v` the same race failed with
exit 1. The pipeline's copies do not pass `-v` (foreman's
`genCloudCopyCommandV2` emits `gsg -m cp` and `gsg -m rsync -r`).

`-m` narrows the window only for small objects. The pool runs 64 chunks at once
by default, so an object over 1 GiB has chunks that open well after the
transfer began: a 14 GB `ssp.csv` has over 800 of them.

This is the gs half of what item 29 records and fixed for oci, which pins every
chunk with `IfMatch` and settles against the first lookup's checksum. s3 reads
by key in the same way.

**Fix:** open each chunk with `Object(prefix).Generation(attrs.Generation)`, so
a chunk fails with 404 once the generation is gone instead of reading the new
one, and verify against `attrs.CRC32C` from that same lookup rather than
looking the name up again in `MustEqualCRC32C`. Otherwise an overwrite landing
after the last chunk makes a correct copy of the old version read as corrupt.

**Fixed in PR #80.** Every chunk opens
`Object(prefix).Generation(attrs.Generation)`, and `-v` settles against
`attrs.CRC32C` from the same lookup instead of looking the name up again. A
chunk that opens after an overwrite now fails -- unless versioning keeps the old
generation, in which case it reads it. Pinned by
`TestDownloadReadsEveryChunkFromTheGenerationItLookedUp`, which fails against
the previous code with the replacement's bytes and no generation on any chunk.
s3 still reads by key. The case is in `uat.sh`: "regression: a download does
not assemble two generations of an object".

---

## 33. An object name containing `..` is written outside the destination

Listings return object names verbatim, and the local path is built with
`filepath.Join` (`common.JoinPath`, `common/path.go:38`, called from
`cmd/rsync.go:60` and through `GetDstPath` from `cmd/cp.go:81`). Join resolves
`..`, and nothing checks that the result is still under the destination.

GCS accepts `..` in a name, and so does `gsutil cp`. Measured: with
`src/ok.txt`, `src/../esc/evil.txt` and `esc/evil.txt` under one prefix, both

```
gsg -m cp -r    gs://…/src dst/cp
gsg -m rsync -r gs://…/src dst/rs
```

exited 0 and wrote `dst/esc/evil.txt`, with the object's content, outside the
destination. The third object is there because the chunk reader goes through
the XML API, whose URL path has its dot segments resolved, so it reads the name
the `..` resolves to. Each extra `../` climbs one more directory, so whoever can
write names into a bucket that gets synced can write files anywhere the syncing
user can. Without that twin the read fails, but the pre-sized `<name>_.gstmp`
has already been created outside the destination (`gcs/gcs.go:412-418`).

**Fix:** for every remote-to-local transfer, require the joined path to stay
under the destination (`filepath.Rel` not starting with `..`), and fail with an
error naming the object. It belongs where `JoinPath` and `GetDstPath` are
called for downloads, so it covers gs, s3 and oci at once.

**Fixed in PR #80,** by refusing rather than skipping. `common.JoinLocalPath`
and `GetLocalDstPath` join as before and return an error naming the object when
the result would leave the directory. `cp` (both download branches and the
intermediate files of an inter-cloud copy) and `rsync`'s download direction
check every destination before fetching anything, so a bad name refuses the
whole transfer instead of whatever share of it had started. Pinned by
`TestJoinLocalPath`, `TestCpRefusesANameThatClimbsOutOfTheDestination` and
`TestDownsyncRefusesANameThatClimbsOutOfTheDestination`. The case is in
`uat.sh`: "regression: an object name cannot place a file outside the
destination".

---

## 34. A `gs://` argument is parsed as a URL, so `%`, `?` and `#` pick another object

`system.ParseFileObject` runs the argument through `url.Parse` and keeps
`u.Path` (`system/system.go:180`, `:206`). An object name is not a URL:

  - `%xx` is decoded: `gs://b/x%20y.txt` addresses `x y.txt`.
  - `?` and `#` start the query and the fragment: `gs://b/q?x.txt` addresses `q`.
  - A `%` that is not an escape fails to parse, `ParseFileObject` returns nil,
    and the caller dereferences it: `gsg stat gs://b/100%.txt` exits 1 with
    `[RECOVERED] with runtime error: invalid memory address or nil pointer
    dereference`.

Measured with both `x y.txt` and `x%20y.txt` in one prefix: `gsg rm
gs://…/x%20y.txt` logged `Removing … prefix[…/x y.txt]`, exited 0 and deleted
`x y.txt`, leaving `x%20y.txt` untouched. `cat` and `stat` of `q?x.txt` and
`a#b.txt` report no such object. Recursive commands are not affected: they
carry the names a listing returned, not a parsed argument, so `cp -r` and
`rsync -r` round-trip all of these names.

**Fix:** split `scheme://authority/rest` by hand and keep `rest` verbatim. The
authority still has to accept oci's `bucket@namespace`, which is what `u.User`
supplies today.

**Case for `uat.sh`:**

```bash
    start "regression: an object argument is a name, not a URL"
    furl="folder_urlname"
    mkdir -p $furl
    echo space > "$furl/x y.txt"
    echo literal > "$furl/x%20y.txt"
    echo query > "$furl/q?x.txt"
    echo fragment > "$furl/a#b.txt"
    # a recursive copy carries the names verbatim, so all four land intact
    ../gsg -m cp -r $furl "$remote_base/$furl" >/dev/null 2>&1
    assertEq "all four names landed" "$(remote_count $furl)" "4"
    assertEq "cat of x%20y.txt reads that object" "$(../gsg cat "$remote_base/$furl/x%20y.txt" 2>/dev/null)" "literal"
    assertEq "cat of q?x.txt" "$(../gsg cat "$remote_base/$furl/q?x.txt" 2>/dev/null)" "query"
    assertEq "cat of a#b.txt" "$(../gsg cat "$remote_base/$furl/a#b.txt" 2>/dev/null)" "fragment"
    assertNoCrash "a lone % is an error, not a crash" ../gsg stat "$remote_base/$furl/100%.txt"
    ../gsg rm "$remote_base/$furl/x%20y.txt" >/dev/null 2>&1 || true
    assertEq "rm of x%20y.txt removed exactly that object" \
        "$(gsutil ls "$remote_base/$furl/" | sed 's|.*/||' | LC_ALL=C sort | tr '\n' ',')" "a#b.txt,q?x.txt,x y.txt,"
    rm -rf $furl
    finish
```

---

## 35. One dropped connection fails a `cp` upload

`cp` sends each file once (`cmd/cp.go:43`, `:61`); `rsync` wraps the same call in
`DoWithRetrySimple` (`cmd/rsync.go:102`). Below that the retries are the
libraries', and storage v1.22.1 with google.golang.org/api v0.93.0 leave gaps:

  - An object written without preconditions is not idempotent to storage
    v1.22.1, so a single-request upload -- anything under the 16 MiB chunk,
    which is most files -- is sent once.
  - The chunks of a resumable upload go through gensupport's `shouldRetry`,
    which in v0.93 counts `ECONNRESET` and `ECONNREFUSED` as transient only on
    Linux (`retryable_linux.go`), and never `EPIPE` or a bare `EOF`.

Measured through `uat/faultproxy`, resetting every open storage connection once,
midway:

| case | macOS | Linux (`xsmtools` image) |
|---|---|---|
| 60 MiB single-stream `cp` | exit 1, `connection reset by peer` | exit 1, `EOF` |
| 100 MiB composite `cp -m` (5 parts, test build) | exit 1, `broken pipe` | exit 1, `EOF` |
| `cp -m -r` of 150 files of 256 KiB | exit 1, 35 of 150 stored | exit 1, 57 of 150 stored |
| `rsync -m -r` of the same | exit 0, 150 stored | exit 0, 150 stored |

The Linux column saw `EOF` where a real network would more likely deliver
`ECONNRESET`, which v0.93 does retry there (see the note in
`uat/faultproxy/main.go`). `EPIPE` and `EOF` would still fail. The newest api
(v0.269) retries `connection reset` and `broken pipe` on every platform, and
`net.ErrClosed`, but still not a bare `EOF`.

**Fix:** wrap `cp`'s `Upload` and `Download` in a retry, as `rsync` does. That
covers every error class at the cost of restarting one file. Upgrading storage
and api would add the finer-grained chunk retries, but v1.22.1 is from 2022,
so that change is larger and should be measured before and after.

**Fixed in PR #80.** `cp` wraps every `Upload` and `Download` -- both
branches each way, and both halves of an inter-cloud copy -- in
`DoWithRetrySimple`, as `rsync` does. Pinned by
`TestCpUploadTriesAgainAfterAFailedAttempt` and
`TestCpDownloadTriesAgainAfterAFailedAttempt`. For a gs download the retry
only takes effect once item 36 is fixed, since a failed chunk still ends the
process before `Download` can return. The library upgrade stays open.

The case is in `uat.sh`: "regression: one dropped connection does not fail a cp
upload". The `fp_*` helpers the fault-injection cases use are there too;
`fp_start` builds and starts `uat/faultproxy` and `fp_stop` ends it.

---

## 36. A gs download chunk that fails exits the process

Item 29 notes this in passing; it has no item of its own. Every error inside a
chunk calls `common.Exit()` (`gcs/gcs.go:427`, `:436`, `:451`, `:457`, `:467`,
`:472`), so `Download` never returns an error that `rsync`'s
`DoWithRetrySimple` could retry, and the `_.gstmp` stays behind.

A dropped connection on its own is survived: the storage reader reopens at the
offset it had reached. What ends the process is an error the reopen cannot get
past. Measured with `uat/faultproxy` refusing storage connections for 5 s in the
middle of a transfer, on macOS and on Linux alike:

  - `gsg -m cp` of 120 MiB: exit 1 at the first refused reopen, with
    `d4.bin_.gstmp` left behind. `cp` never removes temp files; only the next
    `rsync` into the same directory does.
  - `gsg -m rsync -r` of 31 objects: exit 1 with 1 of 31 files in place and 30
    `_.gstmp`. A second run completed and removed them.

One failed 16 MiB chunk costs the whole command -- for a 14 GB object,
everything downloaded so far.

**Fix:** have `Download` return the first chunk error: record it, cancel the
other chunks, remove the temp file. `oci.Download` does this since #74. Retry
the chunk itself before giving up on the file. Mind the schedule:
`DoWithRetrySimple` waits 0, 100 and 200 ms, so it bridges one refused
reconnect but not an outage of a few seconds, and the second half of the case
below asks for that. s3 has the same `common.Exit()` calls.

**Case for `uat.sh`** (the `fp_*` helpers in `uat.sh`):

```bash
    start "regression: a download chunk that loses its connection is retried, not abandoned"
    fp_start
    fdl="folder_dlfault"
    mkdir -p $fdl
    dd if=/dev/urandom of=$fdl/obj.bin bs=1048576 count=120 2>/dev/null
    gsutil -q cp $fdl/obj.bin "$remote_base/$fdl/src/obj.bin"

    # every connection reset, and one reconnect refused: one chunk's reopen fails
    HTTPS_PROXY=$fp_proxy ../gsg -m cp "$remote_base/$fdl/src/obj.bin" $fdl/a.bin >/dev/null 2>&1 &
    pid=$!
    fp_wait down $((40 << 20))
    fp "resetrefuse?n=1" >/dev/null
    wait $pid && rc=0 || rc=$?
    assertEq "a refused reconnect costs a retry, not the download" "$rc" "0"
    assertOk "and the file is whole" cmp $fdl/obj.bin $fdl/a.bin
    assertEq "and no temp file is left" "$(ls $fdl | grep -c gstmp || true)" "0"

    # storage unreachable for 5 s: what a retry schedule has to bridge
    fp reset >/dev/null
    HTTPS_PROXY=$fp_proxy ../gsg -m rsync -r "$remote_base/$fdl/src" $fdl/sync >/dev/null 2>&1 &
    pid=$!
    fp_wait down $((40 << 20))
    fp "mode?set=refuse" >/dev/null
    sleep 5
    fp "mode?set=pass" >/dev/null
    wait $pid && rc=0 || rc=$?
    assertEq "rsync bridges a 5 s outage" "$rc" "0"
    assertOk "and the file is whole" cmp $fdl/obj.bin $fdl/sync/obj.bin
    fp_stop
    rm -rf $fdl
    finish
```

---

## 37. A composite upload killed by a signal leaves its parts

gsg installs no signal handler. A SIGTERM -- a pod eviction, `foremankill.sh`, a
node drain -- ends the process where it stands, and the parts already committed
stay beside the object as `<object>.gsg-part-<uid>-NN`, with nothing logged. The
sweep from #75 and #77 runs only when an attempt fails inside the process.

Measured on Linux in the `xsmtools` image, a 100 MiB file in 5 parts (test
build): SIGTERM once the bucket showed committed parts (2 by then), exit 143,
and 4 parts of 20 MiB left in the bucket. At the real threshold a part is up to 1/32 of the
file -- about 440 MB each for a 14 GB `ssp.csv`.

Storage is not the only cost. The parts are ordinary objects, so a later
`rsync -r` of that directory downloads them, and anything reading the directory
sees them.

**Fix:** on SIGTERM and SIGINT, cancel the uploads and run the same sweep before
exiting, inside the grace period (30 s by default in Kubernetes). Separately,
`ls`, `cp -r` and `rsync -r` could skip names matching
`*.gsg-part-<16 hex>-<2 digits>`, so that leftovers at least do not spread.
Neither covers SIGKILL or an OOM kill; keeping the parts under a prefix that
listings never return would.

**Case for `uat.sh`:**

```bash
    if [[ "$mode" == "gs" ]]
    then
    start "regression: a composite upload terminated by SIGTERM leaves no parts"
    # 600 MiB is 3 parts at the 256 MiB threshold; -c 2 runs two at a time, so
    # two are committed while the third is still going
    fsig="folder_sigterm"
    mkdir -p $fsig
    dd if=/dev/urandom of=$fsig/big.bin bs=1048576 count=600 2>/dev/null
    ../gsg -m -c 2 cp $fsig/big.bin "$remote_base/$fsig/big.bin" >/dev/null 2>&1 &
    pid=$!
    parts=0
    while kill -0 $pid 2>/dev/null && [[ $parts -eq 0 ]]
    do
        sleep 1
        parts=$(gsutil ls "$remote_base/$fsig/" 2>/dev/null | grep -c 'gsg-part-' || true)
    done
    if [[ $parts -eq 0 ]]
    then
        echo "FATAL: the upload ended before any part was committed, so this proved nothing"
        exit 1
    fi
    kill -TERM $pid
    wait $pid || true
    sleep 10   # whatever cleanup the signal starts may still be finishing
    assertEq "no part is left after SIGTERM" \
        "$(gsutil ls "$remote_base/$fsig/" 2>/dev/null | grep -c 'gsg-part-' || true)" "0"
    rm -rf $fsig
    finish
    fi
```

---

## 38. `goog-reserved-file-mtime` is nanoseconds here and seconds everywhere else

gsg writes `modTime.UnixNano()` (`gcs/gcs.go:654`, `:816`) and reads the value
back as nanoseconds (`time.Unix(0, ts)`, `gcs/gcs.go:1092`). gsutil (`cp -P`,
`rsync -P`) writes and reads the same key in seconds.

Measured with a file whose mtime is 2023-01-02 03:04:05 (1672599845):

  - uploaded with `gsutil cp -P`, which stored `1672599845`, then downloaded
    with `gsg cp`: local mtime 1, that is 1970-01-01 00:00:01.
  - uploaded with `gsg cp`, which stored `1672599845000000000`, then downloaded
    with `gsutil cp -P`: local mtime 9223372036, that is the year 2262.

`rsync` is not confused, because gsg reads its own value back consistently.
The damage is the mtime on disk, which `make`, `find -newer` and anything else
reading it then get wrong. It has been this way since the metadata was
introduced (e3eb82e, 2023).

**Fix:** write seconds, as the key's other writers do. Read a value below
about 1e11 as seconds and anything larger as nanoseconds, so objects written by
earlier gsg versions keep their mtime. With seconds on the object, `Attrs.Same`
must compare mtimes at whole seconds too, or every file with a fractional
mtime would look changed on every `rsync`; the last assertion guards that.

**Case for `uat.sh`:**

```bash
    if [[ "$mode" == "gs" ]]
    then
    start "regression: goog-reserved-file-mtime means seconds, as gsutil writes it"
    fmt="folder_mtime"
    mkdir -p $fmt ${fmt}_frac
    echo hello > $fmt/a.txt
    touch -t 202301020304.05 $fmt/a.txt
    want=$(stat -f%m $fmt/a.txt)
    gsutil -q cp -P $fmt/a.txt "$remote_base/$fmt/by_gsutil.txt"
    ../gsg cp "$remote_base/$fmt/by_gsutil.txt" $fmt/from_gsutil.txt >/dev/null 2>&1
    assertEq "gsg restores the mtime gsutil stored" "$(stat -f%m $fmt/from_gsutil.txt)" "$want"
    ../gsg cp $fmt/a.txt "$remote_base/$fmt/by_gsg.txt" >/dev/null 2>&1
    assertEq "gsg stores the mtime in seconds" \
        "$(gsutil stat "$remote_base/$fmt/by_gsg.txt" | awk -F: '/goog-reserved-file-mtime/{gsub(/[[:space:]]/, "", $2); print $2}')" "$want"
    # an object written by an earlier gsg carries nanoseconds and must still read back
    gsutil -q setmeta -h "x-goog-meta-goog-reserved-file-mtime:${want}000000000" "$remote_base/$fmt/by_gsutil.txt"
    ../gsg cp "$remote_base/$fmt/by_gsutil.txt" $fmt/old_style.txt >/dev/null 2>&1
    assertEq "a nanosecond value from an earlier gsg still restores the mtime" "$(stat -f%m $fmt/old_style.txt)" "$want"
    # a sub-second mtime must not make every rsync copy the file again
    echo x > ${fmt}_frac/f.txt
    python3 -c "import os; t = 1672599845123456789; os.utime('${fmt}_frac/f.txt', ns=(t, t))"
    ../gsg rsync -r ${fmt}_frac "$remote_base/${fmt}_frac" >/dev/null 2>&1
    assertEq "a sub-second mtime does not make the next rsync copy it again" \
        "$(../gsg rsync -r ${fmt}_frac "$remote_base/${fmt}_frac" 2>&1 | grep -c 'No diff detected')" "1"
    rm -rf $fmt ${fmt}_frac
    finish
    fi
```

---

## 39. `uat.sh` never exercises the gs composite upload

The large-upload case skips gs with "the gs writer already chunks internally;
unchanged here" (`uat.sh:1121`). Since #68, a gs upload over 256 MiB with `-m`
goes as parallel composed parts, and #75 and #77 rewrote how a failed attempt
cleans up after them. So the gs upload code that changed most has no uat
coverage: the 2026-09-19 run passed all 156 gs assertions without reaching it.

Checked by hand on fbdc019, all passing:

  - 300 MiB at the real threshold: 2 parts, CRC32C and
    `goog-reserved-file-mtime` as sent, the same Content-Type as a single
    stream, and a second `rsync` that is a no-op.
  - With scratch builds that lower `compositeMinSize` (24 MiB keeps the parts
    over the 16 MiB chunk, so they still go resumable, as real parts do):
    exactly at the threshold (one stream), one byte over (2 parts), the 32-part
    cap, and `cp -r` of three composite files with `-c 2` and `-c 3` without a
    deadlock.
  - The failure #75 was written for, reproduced with `uat/faultproxy`: the
    final chunks reach the service, their answers are dropped, then a
    reconnect is refused. The service had committed parts whose writers saw an
    error. On macOS and on Linux, the bucket's soft-delete records showed all 4
    such parts swept within 2 s of the failure.
  - An outage long enough for the sweep's own requests to be refused left 2
    parts behind, both named in the failure log.

**Case for `uat.sh`**, the gs branch of that case. Its last assertion is
item 40 and fails until that is fixed. The sweep assertion uses the `fp_*`
helpers in `uat.sh`:

```bash
    if [[ "$mode" == "gs" ]]
    then
    # 300 MiB: over the 256 MiB threshold, so -m sends 2 composed parts.
    # Random, for the reason given for the s3 and oci branch below.
    fcomp="folder_composite"
    mkdir -p $fcomp
    dd if=/dev/urandom of=$fcomp/big.bin bs=1048576 count=300 2>/dev/null
    ../gsg -m cp $fcomp/big.bin "$remote_base/$fcomp/big.bin" >/dev/null 2>&1
    assertEq "the object stored the whole file" "$(remote_size $fcomp/big.bin)" "$(stat -f%z $fcomp/big.bin)"
    assertEq "and it was composed from 2 parts" \
        "$(gsutil stat "$remote_base/$fcomp/big.bin" | awk '/Component-Count:/{print $2}')" "2"
    assertEq "and no part is left beside it" "$(gsutil ls "$remote_base/$fcomp/" | grep -c 'gsg-part-' || true)" "0"
    assertEq "cp -v verifies the download" \
        "$(../gsg -m cp -v "$remote_base/$fcomp/big.bin" ./comp_down.bin 2>&1 | grep -c 'CRC32C checking success')" "1"
    assertOk "and the download matches byte for byte" cmp $fcomp/big.bin ./comp_down.bin
    rm -rf ${fcomp}_sync && mkdir -p ${fcomp}_sync
    ../gsg -m rsync -r "$remote_base/$fcomp" ${fcomp}_sync >/dev/null 2>&1
    assertEq "a second rsync copies nothing" \
        "$(../gsg -m rsync -r "$remote_base/$fcomp" ${fcomp}_sync 2>&1 | grep -c 'No diff detected')" "1"

    # #75/#77: a part the service committed while its writer saw an error is
    # still swept. 2 parts of 150 MiB go as 9 chunks of 16 MiB and a final
    # 6 MiB each: drop the answers once the full chunks are out, so the final
    # chunks are committed unheard, then refuse one reconnect.
    fp_start
    HTTPS_PROXY=$fp_proxy ../gsg -m cp $fcomp/big.bin "$remote_base/$fcomp/swept.bin" >/dev/null 2>&1 &
    pid=$!
    fp_wait up $((290 << 20))
    fp "mode?set=dropdown" >/dev/null
    sleep 20
    fp "resetrefuse?n=1" >/dev/null
    wait $pid || true
    sleep 5
    assertEq "a failed attempt leaves no part, even one committed unheard" \
        "$(gsutil ls "$remote_base/$fcomp/" | grep -c 'gsg-part-' || true)" "0"

    fp_stop
    # item 40: a name the part suffix would push past 1024 bytes
    long=$(printf 'n%.0s' $(seq 1 $((1000 - ${#testid} - ${#fcomp} - 2))))
    assertOk "a 1000-byte name uploads with -m above the threshold" \
        ../gsg -m cp $fcomp/big.bin "$remote_base/$fcomp/$long"
    rm -rf $fcomp ${fcomp}_sync comp_down.bin
    finish
    else
```

---

## 40. A composite part name can pass the 1024-byte limit

Parts are named `<object>.gsg-part-<16 hex>-NN` (`gcs/gcs.go:747`), 29 bytes
longer than the object. An object name over 995 bytes therefore uploads as a
single stream but fails with `-m` above the threshold. Measured with a
1000-byte name and a test build: `Error 400: The maximum object length is 1024
characters, but got a name with 1030 characters`, exit 1, no part left.

**Fix:** fall back to a single stream when the part names would not fit, or
give the parts short names of their own -- the separate prefix item 37 wants
would do both. The case is the last assertion of item 39's.

## 41. `cp -` reads all of stdin into memory and leaves it in /tmp

`parseStdIn` (`cmd/cp.go:308`) calls `io.ReadAll` on stdin, writes the result to
`/tmp/<UnixNano>` and never removes it. So `gsg cp - dst` holds the whole input
in memory, and every call leaves a copy in `/tmp`. Measured: each `cp -` of
5 MB left a 5,000,000-byte file behind.

**Fix:** stream stdin into `os.CreateTemp` and remove the file after the copy,
as `oci/cat.go:207` already does for its own spool.

**Case for `uat.sh`:**

```bash
    start "regression: cp - leaves nothing in /tmp"
    snapshotTmp
    head -c 5000000 /dev/urandom > .stdin.bin
    ../gsg cp - "$remote_base/stdin.bin" < .stdin.bin >/dev/null 2>&1
    ls /tmp > .tmp_after 2>/dev/null || true
    assertEq "no spool file is left in /tmp" \
        "$(comm -13 .tmp_before .tmp_after | grep -cE '^[0-9]{19}$' || true)" "0"
    assertEq "and the object holds the input" "$(remote_size stdin.bin)" "5000000"
    rm -f .stdin.bin
    finish
```

## 42. On a bucket with soft delete, composite parts are billed a second time

The parts of a composite upload are deleted once the object is composed. On a
bucket with a soft-delete policy the deleted parts are kept, and billed, for the
retention period, so an upload of N bytes also costs N bytes of soft-deleted
storage for that long. `gs://gsg-uat` keeps them 7 days; `tomtom-transformer-bom`
has soft delete off, so the pipeline is not affected today.

Not a defect so much as a cost to know about before pointing `-m` uploads of
large files at such a bucket. `bucketAllowsCompose` already looks at the bucket
to rule out a retention policy; it could log the soft-delete retention the same
way.

