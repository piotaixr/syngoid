package engine

import "strings"

// Order returns a replication order for states and the dependencies that
// produced it: deps[i] lists the indices state i must wait for. A state
// waits for its parent when the parent is in states and has no target yet,
// or always when forceDelete is set (a parent's ForceDestroyTarget destroys
// its target recursively, children included), and for its origin dataset
// when the origin is in scope. order is a
// topological order that always takes the lowest-index ready state, so
// independent datasets keep their listing order. On a dependency cycle
// (which should not happen) the remaining states follow in index order.
func Order(states []DatasetState, forceDelete bool) (order []int, deps [][]int) {
	index := make(map[string]int, len(states))
	for i, s := range states {
		index[s.Source] = i
	}
	deps = make([][]int, len(states))
	for i, s := range states {
		if p, ok := parentName(s.Source); ok {
			if j, found := index[p]; found && (forceDelete || !states[j].TargetExists) {
				deps[i] = append(deps[i], j)
			}
		}
		if s.OriginInScope {
			origin, _, _ := strings.Cut(s.Origin, "@")
			if j, found := index[origin]; found && j != i && !contains(deps[i], j) {
				deps[i] = append(deps[i], j)
			}
		}
	}

	done := make([]bool, len(states))
	order = make([]int, 0, len(states))
	for len(order) < len(states) {
		next := -1
		for i := range states {
			if !done[i] && ready(deps[i], done) {
				next = i
				break
			}
		}
		if next < 0 {
			for i := range states {
				if !done[i] {
					order = append(order, i)
				}
			}
			break
		}
		done[next] = true
		order = append(order, next)
	}
	return order, deps
}

func ready(deps []int, done []bool) bool {
	for _, d := range deps {
		if !done[d] {
			return false
		}
	}
	return true
}

func contains(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func parentName(ds string) (string, bool) {
	i := strings.LastIndexByte(ds, '/')
	if i < 0 {
		return "", false
	}
	return ds[:i], true
}
