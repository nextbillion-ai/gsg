package cmd

import (
	"testing"

	"github.com/nextbillion-ai/gsg/system"
	"github.com/stretchr/testify/assert"
)

type fakeMvSystem struct {
	system.ISystem
	scheme string
}

func (f *fakeMvSystem) Scheme() string { return f.scheme }

func fo(sys system.ISystem, bucket, prefix string) *system.FileObject {
	return &system.FileObject{System: sys, Bucket: bucket, Prefix: prefix, Remote: true}
}

// mv is a copy followed by a delete of the source, so a destination that is
// the source, or inside it, means the delete removes what the copy wrote.
// gsutil refuses the identical case with "are the same file - abort"; it
// allows the nested one because its copy nests the source directory, which
// gsg's cp -r does not.
func TestWouldDestroySource(t *testing.T) {
	gs := &fakeMvSystem{scheme: "gs"}
	s3 := &fakeMvSystem{scheme: "s3"}

	assert.True(t, wouldDestroySource(fo(gs, "b", "a.txt"), fo(gs, "b", "a.txt")))

	// A trailing slash is one keystroke and names the same place, because
	// gsg's cp -r copies a directory's contents rather than the directory.
	assert.True(t, wouldDestroySource(fo(gs, "b", "d"), fo(gs, "b", "d/")))
	assert.True(t, wouldDestroySource(fo(gs, "b", "d/"), fo(gs, "b", "d")))
	assert.True(t, wouldDestroySource(fo(gs, "b", "d/"), fo(gs, "b", "d/")))

	// The destination inside the source. gsg's cp -r flattens, so d/a.txt and
	// d/sub/a.txt would both want to become d/sub/a.txt -- measured, that lost
	// the first one.
	assert.True(t, wouldDestroySource(fo(gs, "b", "d"), fo(gs, "b", "d/sub")))
	assert.True(t, wouldDestroySource(fo(gs, "b", "d/"), fo(gs, "b", "d/sub/deeper")))

	for _, c := range []struct {
		a, b *system.FileObject
		why  string
	}{
		{fo(gs, "b", "a.txt"), fo(gs, "b", "b.txt"), "different objects"},
		{fo(gs, "b", "a.txt"), fo(gs, "other", "a.txt"), "different buckets"},
		{fo(gs, "b", "a.txt"), fo(s3, "b", "a.txt"), "different backends"},
		// A sibling whose name merely begins with the source's.
		{fo(gs, "b", "d"), fo(gs, "b", "dsub"), "a sibling with a shared prefix"},
		{fo(gs, "b", "a.txt"), fo(gs, "b", "a.txt.bak"), "a longer name"},
		// A path that did not parse names nothing.
		{nil, fo(gs, "b", "a.txt"), "nil source"},
		{fo(gs, "b", "a.txt"), nil, "nil destination"},
		{&system.FileObject{}, fo(gs, "b", "a.txt"), "source with no backend"},
	} {
		assert.False(t, wouldDestroySource(c.a, c.b), c.why)
	}
}
