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
| 9 | open | yes -- 1.8x slower, ~1005 extra calls per 1005 objects | fix eventually |
| 10 | open | yes, but unreachable from the CLI | low |
| 11 | open | yes, but needs a marker carrying bytes | low |
| 12 | PR #42 | yes -- 8 data races under -m | fix |
| 13 | PR #43 | n/a -- duplication, one copy with a flaw | fix |
| 14 | PR #46 | yes -- 301 MovedPermanently across regions | fix, with 3 |
| 15 | PR #44 | yes -- 8 of 8 processes acquire the same lock | fix, folded into #44 |
| 16 | open | yes -- one receipt file for both schemes | low, needs the same bucket and key on both |
| 17 | open | yes -- keys with # or non-ascii silently fail to copy | fix, small |
| 18 | open | yes -- two checksum-less objects compare equal | fix with 4's other half |

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

**Reproduced** against real S3, copying each key from one prefix to another:

```
"plain.txt"        landed: yes
"with space.txt"   landed: yes
"hash#tag.txt"     landed: NO
"café.txt"         landed: NO
```

No error surfaces at the gsg level, so the object simply is not there
afterwards. `Copy` is what `cp` and `rsync` use between two cloud paths, so a
tree containing such a name syncs incompletely.

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
