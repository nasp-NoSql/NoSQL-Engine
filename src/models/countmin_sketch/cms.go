package countmin_sketch

type CountMinSketch struct {
	w      uint
	d      uint
	table  [][]uint
	hashes []HashWithSeed
}

// Initialize creates a new CMS with error rate epsilon, confidence 1-delta
func (cms *CountMinSketch) Initialize(epsilon, delta float64) {
	cms.w = CalculateW(epsilon)
	cms.d = CalculateD(delta)

	cms.table = make([][]uint, cms.d)
	for i := range cms.table {
		cms.table[i] = make([]uint, cms.w)
	}

	cms.hashes = CreateHashFunctions(cms.d)
}

// Add increments counters for the data
func (cms *CountMinSketch) Add(data []byte) {
	for i := uint(0); i < cms.d; i++ {
		hashVal := cms.hashes[i].Hash(data)
		idx := hashVal % uint64(cms.w)
		cms.table[i][idx]++
	}
}

// Estimate returns approximate frequency count of the data
func (cms *CountMinSketch) Estimate(data []byte) uint {
	min := ^uint(0) // max uint
	for i := uint(0); i < cms.d; i++ {
		hashVal := cms.hashes[i].Hash(data)
		idx := hashVal % uint64(cms.w)
		if cms.table[i][idx] < min {
			min = cms.table[i][idx]
		}
	}
	return min
}
