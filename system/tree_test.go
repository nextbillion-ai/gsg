package system

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// An object directly under base has no intermediate directories. The dirs[1:]
// this replaced panicked with "slice bounds out of range [1:0]" on exactly this.
func TestGetAllParentsOfDirectChildIsEmpty(t *testing.T) {
	assert.Equal(t, []string{}, GetAllParents("dir/file.txt", "dir"))
	assert.Equal(t, []string{}, GetAllParents("dir/file.txt", "dir/"))
	assert.Equal(t, []string{}, GetAllParents("file.txt", ""))
	assert.Equal(t, []string{"dir/sub/"}, GetAllParents("dir/sub/file.txt", "dir"))
	assert.Equal(t, []string{"a/b/c/d/", "a/b/c/d/e/"}, GetAllParents("a/b/c/d/e/f.txt", "a/b/c"))
}

// sizesByName flattens a tree so assertions can address nodes by name.
func sizesByName(t *testing.T, root *DUTree) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, du := range root.ToDiskUsages() {
		out[du.Name] = du.Size
	}
	return out
}

func TestDUTreeAddDirectChild(t *testing.T) {
	root := NewDUTree("dir", 0, true)
	assert.NotPanics(t, func() {
		root.Add("dir/file.txt", 10, "dir")
		root.Add("dir/other.txt", 32, "dir")
	})

	sizes := sizesByName(t, root)
	assert.Equal(t, int64(10), sizes["dir/file.txt"])
	assert.Equal(t, int64(32), sizes["dir/other.txt"])
	assert.Equal(t, int64(42), sizes["dir/"], "root must total its children")
}

// dirs[1:] dropped the shallowest intermediate directory, so a nested object
// was attached too high up and a whole level went missing from du output.
func TestDUTreeAddKeepsEveryDirectoryLevel(t *testing.T) {
	root := NewDUTree("a/b/c", 0, true)
	root.Add("a/b/c/d/e/f.txt", 7, "a/b/c")

	sizes := sizesByName(t, root)
	assert.Contains(t, sizes, "a/b/c/d/")
	assert.Contains(t, sizes, "a/b/c/d/e/")
	assert.Contains(t, sizes, "a/b/c/d/e/f.txt")
	assert.Equal(t, int64(7), sizes["a/b/c/d/"])
	assert.Equal(t, int64(7), sizes["a/b/c/d/e/"])
	assert.Equal(t, int64(7), sizes["a/b/c/"])
}

// A mix of depths under one prefix, which is what du -s actually sums.
func TestDUTreeAddMixedDepths(t *testing.T) {
	root := NewDUTree("data", 0, true)
	root.Add("data/top.txt", 1, "data")
	root.Add("data/sub/a.txt", 2, "data")
	root.Add("data/sub/b.txt", 4, "data")
	root.Add("data/sub/deep/c.txt", 8, "data")

	sizes := sizesByName(t, root)
	assert.Equal(t, int64(1), sizes["data/top.txt"])
	assert.Equal(t, int64(8), sizes["data/sub/deep/"])
	assert.Equal(t, int64(14), sizes["data/sub/"])
	assert.Equal(t, int64(15), sizes["data/"])

	// du -s prints only the last entry, so it must be the root total.
	dus := root.ToDiskUsages()
	assert.Equal(t, "data/", dus[len(dus)-1].Name)
	assert.Equal(t, int64(15), dus[len(dus)-1].Size)
}

// du against a bucket root passes an empty prefix.
func TestDUTreeAddAtBucketRoot(t *testing.T) {
	root := NewDUTree("", 0, true)
	assert.NotPanics(t, func() {
		root.Add("top.txt", 3, "")
		root.Add("d/nested.txt", 5, "")
	})

	sizes := sizesByName(t, root)
	assert.Equal(t, int64(3), sizes["top.txt"])
	assert.Equal(t, int64(5), sizes["d/"])
	assert.Equal(t, int64(8), sizes[""])
}

// namesOf keeps duplicates, which a map would hide.
func namesOf(t *testing.T, root *DUTree) []string {
	t.Helper()
	out := []string{}
	for _, du := range root.ToDiskUsages() {
		out = append(out, du.Name)
	}
	return out
}

// S3 common prefixes and zero-byte directory markers arrive as names ending in
// "/". The walk already lands on that directory's node, so adding a leaf for it
// as well would emit the same name twice.
func TestDUTreeAddDirectoryMarkerIsNotDuplicated(t *testing.T) {
	root := NewDUTree("data", 0, true)
	root.Add("data/sub/", 0, "data")
	root.Add("data/sub/a.txt", 5, "data")

	names := namesOf(t, root)
	assert.Equal(t, []string{"data/sub/a.txt", "data/sub/", "data/"}, names)
	assert.Equal(t, int64(5), sizesByName(t, root)["data/"])
}

// A marker for the prefix itself must not become a child of the root.
func TestDUTreeAddMarkerForBaseItself(t *testing.T) {
	root := NewDUTree("data", 0, true)
	root.Add("data/", 0, "data")
	root.Add("data/a.txt", 9, "data")

	assert.Equal(t, []string{"data/a.txt", "data/"}, namesOf(t, root))
	assert.Equal(t, int64(9), sizesByName(t, root)["data/"])
}

// A marker carrying bytes still contributes them exactly once.
func TestDUTreeAddDirectoryMarkerSizeCountedOnce(t *testing.T) {
	root := NewDUTree("data", 0, true)
	root.Add("data/sub/", 3, "data")
	root.Add("data/sub/a.txt", 5, "data")

	sizes := sizesByName(t, root)
	assert.Equal(t, int64(8), sizes["data/sub/"])
	assert.Equal(t, int64(8), sizes["data/"])
	assert.Equal(t, 3, len(namesOf(t, root)))
}

// Listings can deliver a marker after the children beneath it, and can repeat
// one. Neither may duplicate a row or double-count bytes.
func TestDUTreeAddMarkerOrderAndRepeats(t *testing.T) {
	root := NewDUTree("data", 0, true)
	root.Add("data/sub/a.txt", 5, "data")
	root.Add("data/sub/", 3, "data")

	assert.Equal(t, []string{"data/sub/a.txt", "data/sub/", "data/"}, namesOf(t, root))
	assert.Equal(t, int64(8), sizesByName(t, root)["data/"])

	// A repeat of a zero-byte marker changes nothing.
	root.Add("data/sub/", 0, "data")
	assert.Equal(t, []string{"data/sub/a.txt", "data/sub/", "data/"}, namesOf(t, root))
	assert.Equal(t, int64(8), sizesByName(t, root)["data/"])
}

func TestDUTreeAddIgnoresEmptyName(t *testing.T) {
	root := NewDUTree("dir", 0, true)
	assert.NotPanics(t, func() { root.Add("", 10, "dir") })
	assert.Equal(t, int64(0), sizesByName(t, root)["dir/"])
}
