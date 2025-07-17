package tests

import (
	"math/bits"
	"nosqlEngine/src/models/simhash"
	"strings"
	"testing"
)

func TestSimHashSimilarity(t *testing.T) {
	textA := "The quick brown fox jumps over the lazy dog"
	textB := "The quick brown fox jumps over a lazy dog"
	textC := "Completely unrelated text that should hash far away"

	var shA, shB, shC simhash.SimHash
	shA.Generate(strings.Split(textA, " "))
	shB.Generate(strings.Split(textB, " "))
	shC.Generate(strings.Split(textC, " "))

	hashA := shA.Hash
	hashB := shB.Hash
	hashC := shC.Hash

	distAB := bits.OnesCount64(hashA ^ hashB)
	distAC := bits.OnesCount64(hashA ^ hashC)

	if distAB >= 10 {
		t.Errorf("Hamming distance too large for similar texts: got %d", distAB)
	}

	if distAC <= 20 {
		t.Errorf("Hamming distance too small for different texts: got %d", distAC)
	}
}

func TestHammingDistance(t *testing.T) {
	var a uint64 = 0b10101010
	var b uint64 = 0b11110000

	expected := 4
	got := bits.OnesCount64(a ^ b)

	if got != expected {
		t.Errorf("Expected Hamming distance %d, got %d", expected, got)
	}
}
