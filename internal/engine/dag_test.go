package engine

import (
	"reflect"
	"testing"
)

func TestOrderParentAndClone(t *testing.T) {
	states := []DatasetState{
		{Source: "tank/c2", Origin: "tank/c1@s", OriginInScope: true}, // clone of clone, listed first
		{Source: "tank/c1", Origin: "tank/p@s", OriginInScope: true},
		{Source: "tank/p"},
		{Source: "tank/p/kid"},
		{Source: "tank/e/kid", TargetExists: false},
		{Source: "tank/e", TargetExists: true},
		{Source: "tank/e/other"},
		{Source: "tank/outside", Origin: "elsewhere/x@s", OriginInScope: false},
	}
	order, deps := Order(states)
	wantDeps := [][]int{{1}, {2}, nil, {2}, nil, nil, nil, nil}
	if !reflect.DeepEqual(deps, wantDeps) {
		t.Fatalf("deps = %v, want %v", deps, wantDeps)
	}
	// c2 waits for c1 which waits for p; ties keep listing order.
	wantOrder := []int{2, 1, 0, 3, 4, 5, 6, 7}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("order = %v, want %v", order, wantOrder)
	}
}

func TestOrderCloneFollowsOriginAndParent(t *testing.T) {
	states := []DatasetState{
		{Source: "tank/a/clone", Origin: "tank/b@s", OriginInScope: true},
		{Source: "tank/a"},
		{Source: "tank/b"},
	}
	order, deps := Order(states)
	if want := [][]int{{1, 2}, nil, nil}; !reflect.DeepEqual(deps, want) {
		t.Fatalf("deps = %v, want %v", deps, want)
	}
	if want := []int{1, 2, 0}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestOrderCycleAppendsRemaining(t *testing.T) {
	states := []DatasetState{
		{Source: "tank/x", Origin: "tank/y@s", OriginInScope: true},
		{Source: "tank/y", Origin: "tank/x@s", OriginInScope: true},
		{Source: "tank/z"},
	}
	order, _ := Order(states)
	if want := []int{2, 0, 1}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}
