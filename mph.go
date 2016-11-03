// Package mph implements a minimal perfect hash table over strings.
package mph

import (
	"sort"

	"github.com/bitmagic/bloom"
)

// A Table is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type Table struct {
	filter    *bloom.Filter
	level0    []uint32
	level0Len int
	level1    []uint32
	level1Len int
}

// Build builds a Table from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func Build(keys []string, loadFactor float32, fpProb float64) *Table {
	if loadFactor > 1.0 || loadFactor == 0.0 {
		loadFactor = 1.0
	}
	tableLen := int(float32(len(keys)) / loadFactor)
	var (
		level0        = make([]uint32, tableLen/4)
		level0Len     = len(level0)
		level1        = make([]uint32, tableLen)
		level1Len     = len(level1)
		sparseBuckets = make([][]int, len(level0))
		zeroSeed      = murmurSeed(0)
	)
	for i, s := range keys {
		n := int(zeroSeed.hash(s)) % level0Len
		sparseBuckets[n] = append(sparseBuckets[n], i)
	}
	var buckets []indexBucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, indexBucket{n, vals})
		}
	}
	sort.Sort(bySize(buckets))

	occ := make([]bool, len(level1))
	var tmpOcc []int
	for _, bucket := range buckets {
		var seed murmurSeed
	trySeed:
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.vals {
			n := int(seed.hash(keys[i])) % level1Len
			if occ[n] {
				for _, n := range tmpOcc {
					occ[n] = false
				}
				seed++
				goto trySeed
			}
			occ[n] = true
			tmpOcc = append(tmpOcc, n)
			level1[n] = uint32(i)
		}
		level0[int(bucket.n)] = uint32(seed)
	}

	filter := bloom.New(len(keys), fpProb)
	for _, key := range keys {
		filter.Add(key)
	}

	return &Table{
		filter:    filter,
		level0:    level0,
		level0Len: level0Len,
		level1:    level1,
		level1Len: level1Len,
	}
}

// Lookup searches for s in t and returns its index and whether it was found.
func (t *Table) Lookup(s string) (n uint32, ok bool) {
	i0 := int(murmurSeed(0).hash(s)) % t.level0Len
	seed := t.level0[i0]
	i1 := int(murmurSeed(seed).hash(s)) % t.level1Len
	n = t.level1[i1]
	return n, t.filter.Has(s)
}

type indexBucket struct {
	n    int
	vals []int
}

type bySize []indexBucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].vals) > len(s[j].vals) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
