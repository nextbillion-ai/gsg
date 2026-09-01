package common

import "testing"

// A part size is only ever raised, never lowered below what a service accepts.
func TestPartGeometryRespectsTheMinimum(t *testing.T) {
	size := int64(100 * 1024 * 1024)
	min := int64(5 * 1024 * 1024)
	ps, _ := PartGeometry(size, 1024, min, 5*1024*1024*1024) // 1 KiB requested, far below the floor
	if ps != min {
		t.Fatalf("part size %d is below the service minimum %d, so every part but the last would be rejected", ps, min)
	}
}

// No preference means the measured default, not zero.
func TestPartGeometryDefaultsWhenUnset(t *testing.T) {
	for _, requested := range []int64{-1, 0} {
		ps, _ := PartGeometry(1<<30, requested, 5*1024*1024, 5*1024*1024*1024)
		if ps != DefaultPartSize {
			t.Errorf("requested %d gave part size %d, want the default %d", requested, ps, DefaultPartSize)
		}
	}
}

// The 10000-part cap is the one that turns a working upload into a failed one,
// so an object too large for the requested part size gets larger parts rather
// than an error.
func TestPartGeometryGrowsRatherThanExceedingTheCap(t *testing.T) {
	// 4 TiB at the default 128 MiB would need 32768 parts.
	size := int64(4) * 1024 * 1024 * 1024 * 1024
	ps, parts := PartGeometry(size, -1, 5*1024*1024, 5*1024*1024*1024)
	if parts > MaxParts {
		t.Fatalf("%d parts exceeds the %d the services allow", parts, MaxParts)
	}
	if ps <= DefaultPartSize {
		t.Errorf("part size stayed at %d; it had to grow to fit within the cap", ps)
	}
	if (size+ps-1)/ps != parts {
		t.Errorf("part count %d does not cover %d bytes at %d per part", parts, size, ps)
	}
}

// Every byte must land in exactly one part, including the awkward sizes.
func TestPartGeometryCoversTheWholeObject(t *testing.T) {
	min := int64(5 * 1024 * 1024)
	for _, size := range []int64{0, 1, min - 1, min, min + 1, DefaultPartSize, DefaultPartSize + 1, 3*DefaultPartSize - 1} {
		ps, parts := PartGeometry(size, -1, min, 5*1024*1024*1024)
		var covered int64
		for i := int64(0); i < parts; i++ {
			off := i * ps
			length := ps
			if off+length > size {
				length = size - off
			}
			if length < 0 {
				length = 0
			}
			covered += length
		}
		if covered != size {
			t.Errorf("size %d: parts cover %d bytes", size, covered)
		}
		if parts < 1 {
			t.Errorf("size %d: %d parts, callers would have no range to send", size, parts)
		}
	}
}

// The bound exists so a recursive copy cannot multiply file concurrency by
// part concurrency, and so a small object does not start eight goroutines to
// send two parts.
func TestPartConcurrencyIsBounded(t *testing.T) {
	if got := PartConcurrency(2); got != 2 {
		t.Errorf("2 parts got concurrency %d, want 2", got)
	}
	if got := PartConcurrency(10000); got > 8 {
		t.Errorf("10000 parts got concurrency %d, which is unbounded fan-out", got)
	}
	if got := PartConcurrency(1); got != 1 {
		t.Errorf("1 part got concurrency %d", got)
	}
}
