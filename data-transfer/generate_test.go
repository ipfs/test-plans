package main

import (
	"context"
	"math/rand"
	"testing"
)

func TestFoo(t *testing.T) {
	f, err := GenerateDeepDagCarFile(context.Background(), rand.New(rand.NewSource(0)), 1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	bs, err := LoadCARBlockstore(f)
	if err != nil {
		t.Fatal(err)
	}

	rts, err := bs.Roots()
	if err != nil {
		t.Fatal(err)
	}

	if len(rts) != 1 {
		t.Fatalf("should have 1 root not %d", len(rts))
	}

	has, err := bs.Has(context.Background(), rts[0])
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatalf("should have root %v", rts[0])
	}
}
