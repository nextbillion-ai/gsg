package linux

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitPaths(t *testing.T) {
	assert.Equal(t, []string{}, splitPaths(nil))
	assert.Equal(t, []string{}, splitPaths([]byte("")))
	assert.Equal(t, []string{"/a"}, splitPaths([]byte("/a\x00")))
	assert.Equal(t, []string{"/a", "/b"}, splitPaths([]byte("/a\x00/b\x00")))
	// The whole point: a newline inside a name stays part of that one path.
	assert.Equal(t, []string{"/a\nb", "/c"}, splitPaths([]byte("/a\nb\x00/c\x00")))
	// Tolerated, though find does not emit them: no trailing NUL, empty entries.
	assert.Equal(t, []string{"/a"}, splitPaths([]byte("/a")))
	assert.Equal(t, []string{"/a", "", "/b"}, splitPaths([]byte("/a\x00\x00/b\x00")))
}

// A failed listing must not look like an empty one: rsync -d deletes whatever
// the source listing omits, so swallowing the error could wipe the destination.
func TestListReportsFailureInsteadOfEmpty(t *testing.T) {
	dir := t.TempDir()
	unreadable := filepath.Join(dir, "denied")
	assert.NoError(t, os.Mkdir(unreadable, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(unreadable, "f.txt"), []byte("x"), 0644))
	assert.NoError(t, os.Chmod(unreadable, 0000))
	t.Cleanup(func() { _ = os.Chmod(unreadable, 0755) })

	if os.Geteuid() == 0 {
		t.Skip("root can read the directory regardless of its mode")
	}

	fos, err := (&Linux{}).List("", dir, true)
	assert.Error(t, err, "an unreadable subdirectory must surface as an error")
	assert.Nil(t, fos)
}

// Listing a path that does not exist yet is a normal rsync destination, and
// must stay an empty success rather than becoming an error.
func TestListOfMissingDirIsEmptyNotAnError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-created-yet")

	fos, err := (&Linux{}).List("", missing, true)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(fos))
}

func prefixes(t *testing.T, dir string, isRec bool) []string {
	t.Helper()
	fos, err := (&Linux{}).List("", dir, isRec)
	assert.NoError(t, err)
	out := []string{}
	for _, fo := range fos {
		rel, relErr := filepath.Rel(dir, fo.Prefix)
		assert.NoError(t, relErr)
		out = append(out, rel)
	}
	sort.Strings(out)
	return out
}

// Splitting find's output on "\n" turned one such file into two paths naming
// nothing, and those reached callers with nil Attributes.
func TestListHandlesNewlineInFilename(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "plain.txt"), []byte("a"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "we\nird.txt"), []byte("bb"), 0644))

	assert.Equal(t, []string{"plain.txt", "we\nird.txt"}, prefixes(t, dir, true))
}

// Every object List hands back must carry attributes; callers dereference them
// without checking (cmd/rsync.go listRelatively writes Attributes.RelativePath).
func TestListAlwaysPopulatesAttributes(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt"} {
		assert.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("x"), 0644))
	}
	assert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "c.txt"), []byte("y"), 0644))

	fos, err := (&Linux{}).List("", dir, true)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(fos))
	for _, fo := range fos {
		assert.NotNil(t, fo, "List must not return nil entries")
		assert.NotNil(t, fo.Attributes, "List must not return entries without attributes: %s", fo.Prefix)
	}
}

// find and the stat are separate steps, so a file can disappear between them.
// It then became a FileObject with nil Attributes, which rsync dereferenced.
// listedFileObject is the seam: List cannot be made to lose the race on demand,
// but this is the exact code path it takes for each listed path.
func TestListedFileObjectSkipsVanishedFile(t *testing.T) {
	dir := t.TempDir()
	l := &Linux{}

	alive := filepath.Join(dir, "alive.txt")
	assert.NoError(t, os.WriteFile(alive, []byte("x"), 0644))
	fo := l.listedFileObject(alive)
	assert.NotNil(t, fo)
	assert.NotNil(t, fo.Attributes)

	// find reported it; it is gone by the time we stat it.
	gone := filepath.Join(dir, "gone.txt")
	assert.NoError(t, os.WriteFile(gone, []byte("x"), 0644))
	assert.NoError(t, os.Remove(gone))
	assert.NotNil(t, l.toFileObject(gone), "the raw conversion still yields an object")
	assert.Nil(t, l.toFileObject(gone).Attributes, "...but with no attributes")
	assert.Nil(t, l.listedFileObject(gone), "so it must not be listed")

	assert.Nil(t, l.listedFileObject(""))
	assert.Nil(t, l.listedFileObject(filepath.Join(dir, "x.txt_.gstmp")))
}

func TestListNonRecursiveStopsAtDepthOne(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "top.txt"), []byte("x"), 0644))
	assert.NoError(t, os.Mkdir(filepath.Join(dir, "sub"), 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "deep.txt"), []byte("y"), 0644))

	assert.Equal(t, []string{"top.txt"}, prefixes(t, dir, false))
	assert.Equal(t, []string{"sub/deep.txt", "top.txt"}, prefixes(t, dir, true))
}

func TestListSkipsTempFiles(t *testing.T) {
	dir := t.TempDir()
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("x"), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(dir, "drop.txt_.gstmp"), []byte("y"), 0644))

	assert.Equal(t, []string{"keep.txt"}, prefixes(t, dir, true))
	assert.Equal(t, 1, len(ListTempFiles(dir, true)))
}
