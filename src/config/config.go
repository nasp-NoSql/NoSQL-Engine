package config

type Config struct {
	DatabasePath    string `json:"database_path"`
	BloomFilterSize int    `json:"bloom_filter_size"`
	MemtableSize    int    `json:"memtable_size"`
	WalPath         string `json:"wal_path"`
	SStablePath     string `json:"sstable_path"`
}
