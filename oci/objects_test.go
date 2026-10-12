package oci

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
)

func summaries(names ...string) []objectstorage.ObjectSummary {
	out := make([]objectstorage.ObjectSummary, 0, len(names))
	for i := range names {
		n := names[i]
		out = append(out, objectstorage.ObjectSummary{Name: &n})
	}
	return out
}

func names(objs []objectstorage.ObjectSummary) []string {
	out := []string{}
	for _, o := range objs {
		out = append(out, *o.Name)
	}
	return out
}

func TestKeepUnderDropsASiblingThatOnlySharesTheSpelling(t *testing.T) {
	// mojo's customize upload: rsync -r -d to .../latest with .../latest2/
	// beside it. The service returned both for the bare prefix.
	const p = "ns/singapore-4w/digraph/5.28-h/latest"
	got := keepUnder(p, summaries(
		p+"/input.osrm.icd",
		p+"/input.osrm.tls",
		p+"2/input.osrm.icd",
		p+"-old/x",
	))
	assert.Equal(t, []string{p + "/input.osrm.icd", p + "/input.osrm.tls"}, names(got))
}

func TestKeepUnderKeepsAnObjectNamedExactlyLikeThePath(t *testing.T) {
	got := keepUnder("some/dir", summaries("some/dir", "some/dir/a", "some/dirt"))
	assert.Equal(t, []string{"some/dir", "some/dir/a"}, names(got))
}

func TestKeepUnderLeavesADirectoryFormOrEmptyPrefixAlone(t *testing.T) {
	in := []string{"some/dir/a", "some/dir2/b"}
	assert.Equal(t, in, names(keepUnder("", summaries(in...))))
	// a trailing slash already excludes siblings at the service
	assert.Equal(t, in, names(keepUnder("some/dir/", summaries(in...))))
}

func TestKeepUnderKeepsANamelessEntryForListToSkip(t *testing.T) {
	got := keepUnder("p", []objectstorage.ObjectSummary{{Name: nil}})
	assert.Len(t, got, 1)
}
