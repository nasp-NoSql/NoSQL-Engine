# NoSQL Engine

## Table of Contents  
- [Introduction](#introduction)  
- [Features](#features)  
- [Installation](#installation)  
- [Usage](#usage) 
- [Architecture Overview](#architecture-overview)  
  - [Write path](#write-path)  
  - [Read path](#read-path)  
  - [Data Storage](#data-storage)  
- [Configuration](#configuration)  
- [License](#license)  

---

## Introduction  
**NoSQL Engine** is a high-performance, production-ready NoSQL database engine written in Go. Designed for speed, reliability, and scalability, it implements modern database technologies including LSM trees, advanced caching, and comprehensive data integrity features. 

### 🎯 Perfect For
- **High-throughput applications** requiring fast writes and efficient reads
- **Time-series data** and append-heavy workloads  
- **Caching layers** and session storage systems
- **IoT data collection** and real-time analytics
- **Microservices** requiring embedded database capabilities

### 🏗️ Architecture Highlights
Our NoSQL engine draws inspiration from industry leaders like **Apache Cassandra**, **RocksDB**, and **DynamoDB**, implementing:
- **Log-Structured Merge (LSM) Trees** for optimal write performance
- **Multi-level caching** with LRU eviction policies  
- **Advanced indexing** with Bloom filters and Merkle trees
- **ACID compliance** through Write-Ahead Logging (WAL)
- **Horizontal scalability** with user-based data partitioning

---

## Features 

### 🚀 Core Features
- **🗃️ SSTable-Based Storage**: Efficient immutable storage with LSM tree architecture
- **📝 Write-Ahead Logging (WAL)**: Protects data integrity by logging changes before committing them to disk
- **🧠 Advanced Memtable System**: Configurable in-memory storage with multiple concurrent instances
- **⚡ Data Caching & Block Management**: LRU-based block cache for optimal performance
- **🔍 Bloom Filters**: Probabilistic data structures for fast key existence checks
- **🌳 Merkle Trees**: Data integrity verification and consistency checks
- **🗜️ SSTable Compaction**: Automated background compaction with configurable thresholds

### 🎯 Query & Data Access Features
- **🔧 Multi-User Support**: User-based data isolation and access control
- **🔄 Prefix Iteration**: Efficient prefix-based key scanning and iteration
- **📄 Range Queries**: Support for key range scanning operations
- **🗑️ Tombstone Deletion**: Proper deletion handling with tombstone markers
- **⚖️ Rate Limiting**: Token bucket algorithm for request throttling

### 🛠️ Advanced Features  
- **📊 Real-time Statistics**: Engine performance metrics and monitoring
- **🎛️ Configurable Architecture**: Extensive configuration options for all components
- **🔧 CLI Interface**: Beautiful command-line interface with interactive operations
- **🧪 Comprehensive Testing**: Full integration and unit test suite
- **📈 LSM Tree Levels**: Multi-level storage optimization for read/write performance

---

## Installation

To get started with this NoSQL engine, follow the steps below to install and set it up on your system.

### Prerequisites
- **Go 1.19+** (Golang)
  Make sure Go is installed on your system. You can check your Go version by running:
  ```bash
  go version
  ```
  If Go is not installed, download it from the official [Go website](https://go.dev/dl/).

- **Git** for cloning the repository
- **Minimum 4GB RAM** for optimal performance  
- **SSD storage** recommended for best I/O performance

### 📦 Steps to Install

1. **Clone the repository**
   ```bash
   git clone https://github.com/IgorAmi52/NoSQL-Engine.git
   ```

2. **Navigate to the project directory**
   ```bash
   cd NoSQL-Engine
   ```

3. **Install dependencies**
   ```bash
   go mod tidy
   ```

4. **Build the engine**
   ```bash
   go build -o nosql-engine ./cmd
   ```

5. **Run the CLI**
   ```bash
   ./nosql-engine
   ```

### 🐳 Docker Support (Optional)
```bash
# Build Docker image
docker build -t nosql-engine .

# Run in container
docker run -it --rm nosql-engine
```
---

 ## Usage

### 🚀 Getting Started

The NoSQL Engine provides a beautiful command-line interface and programmatic API for database operations.

#### **📋 Complete Usage Guide**
For detailed usage instructions, examples, and advanced operations, please see our comprehensive usage guide:

**👉 [CLI_USAGE.md](./CLI_USAGE.md)**

This guide covers:
- Interactive CLI commands and examples
- Programmatic API usage
- Integration testing procedures  
- Performance optimization tips
- Troubleshooting and best practices

#### **🏃 Quick Start**
```bash
# Build and run the CLI
go build -o nosql-engine ./cmd
./nosql-engine

# Or run directly
go run ./cmd
```

---
 
 ## Architecture Overview 
 
 The NoSQL Engine implements a **Log-Structured Merge (LSM) Tree** architecture designed for high-throughput write operations and efficient reads. The system separates write and read paths to optimize performance for different access patterns.
 
### **Write Path** ✍️: 
  Optimized for fast data ingestion and durability guarantees.      
 
 ![write path](/assets/write%20path.png)

**1. Write-Ahead Log (WAL)**
- When a user sends a **PUT** or **DELETE** request, it is first logged in the Write-Ahead Log (WAL)
- WAL ensures durability by persisting operations before applying them to in-memory structures
- Implements **segmented logging** with fixed-size segments containing a configurable number of records
- Each WAL record includes CRC for data integrity verification
- WAL segments cannot be deleted until data is permanently persisted in SSTables

**2. Memtable**
- After WAL confirms the write, data is added to the **Memtable** - a strictly in-memory structure
- Implemented as a hash map with configurable maximum size (specified by number of elements)
- When the predefined Memtable size is reached, values are sorted by key and a new SSTable is created on disk
- During system startup, Memtable is populated with records from WAL for crash recovery

**3. SSTable Creation & Compaction**
- Sorted data from Memtable is written to disk as immutable **SSTables**
- After SSTable creation, the system checks if compaction conditions are met
- **Size-tiered compaction algorithm** is triggered when thresholds are exceeded
- Compactions on one level can trigger compactions on subsequent levels in the LSM tree

**4. Block Manager**
- Manages all disk I/O operations using fixed-size blocks (4KB, 8KB, or 16KB)
- All file access must go through the Block Manager layer
- Supports block-level reading and writing with configurable block sizes
- Integrates with Block Cache for optimized performance
 
### **Read Path** 📖: 
  Multi-level search strategy optimized for fast data retrieval.
 
   ![read path](/assets/read%20path.png)

**Read Operation Flow:**

**1. Memtable Check**
- When a user sends a **GET** request, first check if the record exists in the Memtable
- If found, return the result immediately (fastest path)

**2. Cache Layer Check** 
- If not in Memtable, check the **Cache structure** (LRU-based block cache)
- Block cache consists of a doubly linked list storing block data and a hash map for constant-time access
- If found in cache, return the result

**3. SSTable Traversal**
- Check SSTables one by one, starting from the most recent
- For each SSTable, load its **Bloom Filter** into memory and query for key presence
- If Bloom Filter indicates the key is definitely not present, skip to the next SSTable
- If the key might be present, check additional structures in the current SSTable

**4. LSM Tree Level Traversal**
- SSTable candidates are determined based on the selected compaction algorithm
- After unsuccessfully searching all SSTable candidates on one LSM tree level, move to the next level
- Process repeats until the key is found or the last level is reached

**5. SSTable Internal Search**
- **Summary Structure**: Check if the key falls within Summary ranges (loaded in memory)
- If within range, find the position in the **Index structure** to access
- **Index Structure**: Find the position in the **Data structure** from which to read the record
- **Data Structure**: Read the actual value and return the response to the user
 
 ---
 
 ## Data Storage Components
 
 The NoSQL Engine implements a sophisticated storage layer with multiple components working together to ensure data durability, integrity, and performance.
 
 ### 🗂️ Write-Ahead Log (WAL)
 
 WAL provides **ACID compliance** and crash recovery capabilities:
 
 ![wal structure](/assets/wal.png)

**Key Features:**
- **Segmented logging**: Each segment contains a fixed number of records (user-configurable)
- **Data integrity**: Every WAL record includes CRC fields for corruption detection
- **Sequential access**: WAL records are read from disk one by one, not loaded entirely into memory
- **Durability guarantee**: WAL segments cannot be deleted until data is persisted in SSTables
- **Crash recovery**: On system startup, Memtable is reconstructed from WAL records

**WAL Record Structure:**
- Timestamp, Key Size, Value Size, Key, Value, CRC, and operation type fields
- Fixed-length segments with configurable block-based sizing
- Supports record fragmentation when necessary, with padding applied where needed

 ### 🧠 Memtable
 
**In-memory structure** optimized for fast writes and reads:
- **Implementation**: Hash map-based structure for O(1) access time
- **Configurable size**: Maximum number of elements specified by user
- **Write operations**: All PUT/DELETE operations first go to Memtable after WAL
- **Crash recovery**: Automatically populated from WAL segments during startup
- **Flush trigger**: When maximum size reached, data is sorted and written to SSTable

 ### 📊 SSTable (Sorted String Table)
 
**Immutable disk-based** storage with multiple specialized components:
 
 ![index](/assets/index.png)

#### **SSTable Components:**

**1. Data Structure**
- Stores actual key-value pairs in sorted order
- Structure can be identical to WAL records or optimized format
- Accessed **block by block** (cannot load entire structure into memory)
- Supports tombstone markers for deleted keys

**2. Filter (Bloom Filter)**
- **Loaded into memory** during read operations
- Probabilistic data structure for all keys in the Data structure
- Eliminates unnecessary disk seeks for non-existent keys
- Configurable false positive rate

**3. Index Structure** 
- Maps every key to its corresponding offset in Data structure
- Contains key and offset pairs for efficient lookups
- Accessed **block by block** to manage memory usage
- Critical for translating key searches to exact data locations

**4. Summary Structure**
- **Sparse index** for the Index structure (loaded into memory)
- Contains boundaries: minimum and maximum key values
- Configurable sparsity level (e.g., every 5th Index entry)
- Enables quick range determination before Index access

**5. Metadata (Merkle Tree)**
- **Data integrity verification** for all values in Data structure
- User can initiate validation operations to detect corruption
- System identifies if and where modifications occurred in data structure
- Essential for distributed system consistency checks
 
### 🔧 LSM Tree Organization & Compaction

**Multi-level storage** optimization for balanced read/write performance:
- **LSM Tree Levels**: User-configurable maximum number of levels
- **Size-tiered Compaction**: When compaction conditions are met, algorithm merges SSTables
- **Level Triggering**: Compactions on one level can cascade to subsequent levels
- **Background Process**: Compaction runs automatically based on configurable thresholds
- **Performance Optimization**: Reduces read amplification by merging overlapping key ranges

 ![index](/assets/lsm tree.png)
 
### 📁 Storage Configuration Options

**Flexible storage formats** to suit different use cases:
- **Single-file SSTable**: All components stored in one file
- **Multi-file SSTable**: Each component (Data, Index, Summary, Filter, Metadata) in separate files
- **Backward Compatibility**: Configuration changes don't affect reading existing SSTables
- **Block-based Access**: All large structures accessed via configurable block sizes (4KB, 8KB, 16KB)

---

### 🔍 SSTable Read Operation Details

#### **Memory vs Disk Components During Reads:**

**🧠 In-Memory Components** (loaded during read operations):
- **Summary**: Sparse key-to-offset mapping for quick Index positioning
- **Metadata**: SSTable configuration and management information  
- **Bloom Filter**: Probabilistic key existence checking to avoid unnecessary disk access
- **Merkle Tree**: Data integrity verification and consistency validation

**💽 On-Disk Components** (accessed on-demand):
- **Index**: Complete key-to-data-offset mapping (accessed via Summary positioning)
- **Data**: Actual key-value pairs (accessed via exact Index offsets)

**⚡ Optimized Access Process:**
1. **Filter Check**: Bloom filter quickly determines if key might exist
2. **Summary Consultation**: If filter indicates possible match, Summary provides Index position
3. **Index Lookup**: Exact data offset retrieved from Index structure  
4. **Data Retrieval**: Direct access to required data without full file scanning

**This multi-layered approach minimizes disk I/O operations and significantly enhances read performance through strategic caching and probabilistic filtering.**


 ---

 ## Configuration

### 🔧 Engine Configuration

The NoSQL engine is highly configurable through the `src/config/config.json` file. Key configuration options include:

#### **Performance Settings**
- **Block Size**: Configurable block size for optimal I/O performance
- **Memtable Size & Count**: Control memory usage and flush frequency  
- **WAL Buffer Size**: Write-ahead log buffer configuration
- **Compaction Threshold**: Automatic SSTable compaction triggers

#### **LSM Tree Configuration** 
- **LSM Levels**: Number of storage levels for optimal read/write balance
- **Compaction Strategy**: Background compaction settings

#### **Filter & Index Settings**
- **Bloom Filter**: False positive rate and expected element count
- **Skip List Levels**: In-memory index structure optimization
- **Prefix Scan**: Min/max prefix length for efficient scanning

#### **Rate Limiting**
- **Token Bucket**: Request throttling with configurable refill rates
- **Max Tokens**: Burst capacity for handling traffic spikes

#### **Storage Configuration**
- **Tombstone Marker**: Configurable deletion marker
- **WAL Segment Size**: Write-ahead log segment management

Example configuration structure:
```json
{
  "BLOCK_SIZE": 4096,
  "MEMTABLE_SIZE": 1000,
  "LSM_LEVELS": 3,
  "COMPACTION_THRESHOLD": 4,
  "BLOOM_FILTER_FALSE_POSITIVE_RATE": 0.01,
  "TOKEN_REFILL_RATE": 0.1,
  "MAX_TOKEN": 1000
}
```

For complete configuration options, see the `src/config/config.json` file.
 
 ---

 ## License

 This project is licensed under the MIT License. You are free to use, modify, and distribute this software under the terms of the MIT License.