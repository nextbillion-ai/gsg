package system

import (
	"slices"
	"strings"
)

type DUTree struct {
	Name     string
	Size     int64
	Children map[string]*DUTree
}

func (t *DUTree) GetSize() int64 {
	total := t.Size
	for _, v := range t.Children {
		total += v.GetSize()
	}
	return total
}

func (t *DUTree) ToDiskUsages() []DiskUsage {
	var res []DiskUsage
	childrenSlice := []*DUTree{}
	for _, v := range t.Children {
		childrenSlice = append(childrenSlice, v)
	}
	if len(childrenSlice) > 0 {
		slices.SortFunc[[]*DUTree](childrenSlice, func(a, b *DUTree) int {
			if len(a.Children) > 0 && len(b.Children) == 0 {
				return 1
			}
			if len(a.Children) == 0 && len(b.Children) > 0 {
				return -1
			}
			return strings.Compare(a.Name, b.Name)
			/*
				until := len(a.Name)
				if until > len(b.Name) {
					until = len(b.Name)
				}
				for i := 0; i < until; i++ {
					if a.Name[i] != b.Name[i] {
						return int(a.Name[i]) - int(b.Name[i])
					}
				}
				return len(a.Name) - len(b.Name)
			*/
		})
		for _, c := range childrenSlice {
			res = append(res, c.ToDiskUsages()...)
		}
	}
	res = append(res, DiskUsage{Name: t.Name, Size: t.GetSize()})
	return res
}

func NewDUTree(name string, size int64, folder bool) *DUTree {
	if folder && len(name) > 0 {
		if name[len(name)-1] != '/' {
			name += "/"
		}
	}
	return &DUTree{
		Name:     name,
		Size:     size,
		Children: map[string]*DUTree{},
	}
}

// Add inserts an object into the tree under base, creating the intermediate
// directories between base and the object on the way down.
//
// An object that sits directly under base has no intermediate directories, so
// GetAllParents returns an empty slice for it. Callers used to skip the first
// entry with dirs[1:], which panicked on exactly that case -- and, for deeper
// objects, silently dropped the topmost directory level from the tree.
func (t *DUTree) Add(name string, size int64, base string) {
	if len(name) == 0 {
		return
	}
	node := t
	for _, dir := range GetAllParents(name, base) {
		child, exists := node.Children[dir]
		if !exists {
			child = NewDUTree(dir, 0, true)
			node.Children[dir] = child
		}
		node = child
	}
	if name[len(name)-1] == '/' {
		// A directory marker or common prefix. GetAllParents ends with the
		// directory itself, so the walk above already landed on its node --
		// adding a leaf here too would emit the same name twice.
		node.Size += size
		return
	}
	node.Children[name] = NewDUTree(name, size, false)
}

func GetAllParents(path, base string) []string {
	res := []string{}
	if base == "" || base[len(base)-1] != '/' {
		base = base + "/"
	}
	for {
		if li := strings.LastIndex(path, "/"); li != -1 {
			parent := path[:li] + "/"
			if parent == base {
				break
			}
			res = append([]string{parent}, res...)
			path = path[:li]
			continue
		}
		break
	}
	return res
}
