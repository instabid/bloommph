// Package mph implements a minimal perfect hash table over strings.
package mph

import (
	"encoding/binary"
	"errors"
	"sort"
)

// A Table is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type Table struct {
	keyHashes []uint64
	level0    []uint32
	level0Len int
	level1    []uint32
	level1Len int
}

// Build builds a Table from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func Build(keys []string, loadFactor float32) *Table {
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

	keyHashes := make([]uint64, len(keys), len(keys))
	for i, key := range keys {
		keyHashes[i] = fnv64a([]byte(key))
	}
	return &Table{
		keyHashes: keyHashes,
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
	return n, fnv64a([]byte(s)) == t.keyHashes[int(n)]
}

type indexBucket struct {
	n    int
	vals []int
}

type bySize []indexBucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].vals) > len(s[j].vals) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Inline implementation of FNV hash
const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

func fnv64a(data []byte) uint64 {
	var hash uint64 = offset64
	for _, c := range data {
		hash ^= uint64(c)
		hash *= prime64
	}
	return hash
}

const word = 64
const bpw = word >> 3
const bphw = word >> 4
const ver = 1

func (t *Table) MarshalBinary() ([]byte, error) {
	size := (1+len(t.keyHashes)+1+1)*bpw + (t.level0Len+t.level1Len)*bphw + 1
	data := make([]byte, size)
	data[0] = ver
	binary.LittleEndian.PutUint64(data[1:], uint64(len(t.keyHashes)))
	binary.LittleEndian.PutUint64(data[1+bpw:], uint64(t.level0Len))
	binary.LittleEndian.PutUint64(data[1+2*bpw:], uint64(t.level1Len))
	start := 1 + 3*bpw
	for i, kh := range t.keyHashes {
		binary.LittleEndian.PutUint64(data[start+i*bpw:], kh)
	}
	start += len(t.keyHashes) * bpw
	for i, v := range t.level0 {
		binary.LittleEndian.PutUint32(data[start+i*bphw:], v)
	}
	start += len(t.level0) * bphw
	for i, v := range t.level1 {
		binary.LittleEndian.PutUint32(data[start+i*bphw:], v)
	}
	return data, nil
}

func (t *Table) UnmarshalBinary(data []byte) error {
	if len(data) < 1+3*bpw {
		return errors.New("mph.UnmarshalBinary: data to short. unknown encoding")
	}
	if data[0] != ver {
		return errors.New("mph.UnmarshalBinary: unknown encoding")
	}
	keyHashesLen := int(binary.LittleEndian.Uint64(data[1:]))
	t.level0Len = int(binary.LittleEndian.Uint64(data[1+bpw:]))
	t.level1Len = int(binary.LittleEndian.Uint64(data[1+2*bpw:]))
	if len(data) < (1+keyHashesLen+1+1)*bpw+(t.level0Len+t.level1Len)*bphw+1 {
		return errors.New("mph.UnmarshalBinary: data to short. unknown encoding")
	}
	t.keyHashes = make([]uint64, keyHashesLen)
	start := 1 + 3*bpw
	for i := 0; i < keyHashesLen; i++ {
		t.keyHashes[i] = binary.LittleEndian.Uint64(data[start+i*bpw:])
	}
	t.level0 = make([]uint32, t.level0Len)
	start += keyHashesLen * bpw
	for i := 0; i < t.level0Len; i++ {
		t.level0[i] = binary.LittleEndian.Uint32(data[start+i*bphw:])
	}
	t.level1 = make([]uint32, t.level1Len)
	start += t.level0Len * bphw
	for i := 0; i < t.level1Len; i++ {
		t.level1[i] = binary.LittleEndian.Uint32(data[start+i*bphw:])
	}
	return nil
}
