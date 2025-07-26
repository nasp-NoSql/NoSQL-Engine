package bloom_filter

type PrefixBloomFilter struct {
	filter    *BloomFilter
	minLength int // minimalna dužina prefiksa
	maxLength int // maksimalna dužina prefiksa
}

func NewPrefixBloomFilter() *PrefixBloomFilter {
	// Proceni broj prefiksa: za svaki ključ, imamo (maxLen - minLen + 1) prefiksa
	estimatedPrefixes := CONFIG.BloomFilterExpectedElements * (CONFIG.MaxPrefixLength - CONFIG.MinPrefixLength + 1)

	return &PrefixBloomFilter{
		filter:    NewBloomFilterWithParams(estimatedPrefixes, CONFIG.BloomFilterFalsePositiveRate),
		minLength: CONFIG.MinPrefixLength,
		maxLength: CONFIG.MaxPrefixLength,
	}
}

func (pf *PrefixBloomFilter) SerializeToByteArray() ([]byte, error) {
	return pf.filter.SerializeToByteArray()
}

func DeserializePrefixBloomFilter(data []byte) (*PrefixBloomFilter, error) {
	filter, err := DeserializeFromByteArray(data)
	if err != nil {
		return nil, err
	}

	return &PrefixBloomFilter{
		filter:    filter,
		minLength: CONFIG.MinPrefixLength,
		maxLength: CONFIG.MaxPrefixLength,
	}, nil
}

func (pf *PrefixBloomFilter) Add(key string) {
	// Dodaj sve prefikse ključa u filter
	keyLen := len(key)
	maxLen := pf.maxLength
	if keyLen < maxLen {
		maxLen = keyLen
	}

	for i := pf.minLength; i <= maxLen; i++ {
		prefix := key[:i]
		pf.filter.Add(prefix)
	}
}

func (pf *PrefixBloomFilter) Contains(prefix string) bool {
	prefixLen := len(prefix)

	// Ako je prefix kraći od minLength, možda postoji
	if prefixLen < pf.minLength {
		return true
	}

	// Ako je prefix duži od maxLength, testiraj maxLength verziju
	if prefixLen > pf.maxLength {
		return pf.filter.Check(prefix[:pf.maxLength])
	}

	// Inače testiraj direktno
	return pf.filter.Check(prefix)
}
