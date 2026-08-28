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
// the source means the delete removes what the copy did not replace. gsutil
// refuses the same case: "are the same file - abort".
func TestIsSamePath(t *testing.T) {
	gs := &fakeMvSystem{scheme: "gs"}
	s3 := &fakeMvSystem{scheme: "s3"}

	assert.True(t, isSamePath(fo(gs, "b", "a.txt"), fo(gs, "b", "a.txt")))

	// A trailing slash is one keystroke and names the same place, because
	// gsg's cp -r copies a directory's contents rather than the directory.
	assert.True(t, isSamePath(fo(gs, "b", "d"), fo(gs, "b", "d/")))
	assert.True(t, isSamePath(fo(gs, "b", "d/"), fo(gs, "b", "d")))
	assert.True(t, isSamePath(fo(gs, "b", "d/"), fo(gs, "b", "d/")))

	for _, c := range []struct {
		a, b *system.FileObject
		why  string
	}{
		{fo(gs, "b", "a.txt"), fo(gs, "b", "b.txt"), "different objects"},
		{fo(gs, "b", "a.txt"), fo(gs, "other", "a.txt"), "different buckets"},
		{fo(gs, "b", "a.txt"), fo(s3, "b", "a.txt"), "different backends"},
		// A destination inside the source is a real move, and one gsutil
		// performs, so it must not be refused here.
		{fo(gs, "b", "d"), fo(gs, "b", "d/sub"), "destination inside the source"},
		// A sibling whose name merely begins with the source's.
		{fo(gs, "b", "d"), fo(gs, "b", "dsub"), "a sibling with a shared prefix"},
		{fo(gs, "b", "a.txt"), fo(gs, "b", "a.txt.bak"), "a longer name"},
		// A path that did not parse names nothing.
		{nil, fo(gs, "b", "a.txt"), "nil source"},
		{fo(gs, "b", "a.txt"), nil, "nil destination"},
		{&system.FileObject{}, fo(gs, "b", "a.txt"), "source with no backend"},
	} {
		assert.False(t, isSamePath(c.a, c.b), c.why)
	}
}
