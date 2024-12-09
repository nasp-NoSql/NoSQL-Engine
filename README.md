# NoSQL Engine

## 🚧 Work in Progress 🚧  
**This project is currently a work in progress (WIP).**  
The features and functionality are being actively developed, and there may be incomplete or experimental components. Please check back regularly for updates.  

### Current Status:  
- Core functionality implemented for Write & Read paths.  
- No standard input, just testing unit functionality.  
- Some features may not yet be fully functional or stable.

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
**NoSQL Engine** is a lightweight, efficient, and customizable NoSQL database engine written in Go. Designed for speed and flexibility, it is ideal for scenarios requiring immutable data storage and fast lookups. Our NoSQL engine draws inspiration from modern NoSQL technologies, including Apache Cassandra, DynamoDB, and similar distributed systems.

---

## Features 

### Available Features 🚀
- **SSTable-Based Storage**: Efficient immutable storage for data.
- **Write-Ahead Logging (WAL)**: Protects data integrity by logging changes before committing them to disk.
- **Bloom Filter for Fast Lookups**: Reduces unnecessary disk reads by filtering out non-matching entries.
- **Merkle Tree for SSTables**: Ensures data integrity by detecting and resolving data corruption.
- **Custom Indexing**: Enables fast and efficient key-value retrieval with an optimized index structure.
- **Data Caching**: In-memory caching for frequently accessed data to improve read performance.

---

### Coming Soon ✨
- **Configurable Options**: Tailor the engine to specific workloads using a flexible configuration file.
- **Advanced Query Language**: A user-friendly query interface for managing and retrieving data.
- **LSM Tree**: Implements a Log-Structured Merge (LSM) tree to optimize write performance and reduce read latency.
- **Compression**: Advanced compression techniques to minimize storage usage without sacrificing performance.
- **Multi-Threaded Searches**: Optimized data retrieval with concurrent query processing for faster performance on large datasets.
- **Time Series Implementation**: Specialized support for storing and querying time-stamped data, optimized for high throughput and real-time analytics.


---

## Installation

To get started with this NoSQL engine, follow the steps below to install and set it up on your system.

### Prerequisites
- **Go** (Golang)
  Make sure Go is installed on your system. You can check your Go version by running:
  ```bash
  go version
If Go is not installed, you can download and install it from the official [Go website](https://go.dev/dl/).

### 📦 Steps to Install

1. Clone the repository
  ```bash
  git clone https://github.com/IgorAmi52/NoSQL-Engine.git
   ```
2. Navigate to the project directory
  ```bash
  cd nosql-engine
   ```
3. Install dependencies
   Use go mod to download and install any dependencies specified in the go.mod file:
  ```bash
  go mod tidy
   ```
---

 ## Usage

 Currently, there is no main entry point for the NoSQL engine. The project is in its early stages, and only unit tests are available for now. You can run the tests to validate the functionality of the engine components.

---
 
 ## Architecture Overview 
 
 The search engine is built around a modular architecture that separates concerns and ensures scalability. It consists of:
 
  ### **Write Path** 🗂️: 
  For ingesting data efficiently and preparing it for search.      
 
 ![write path](/assets/write%20path.png)
 
- **Memtable**:  
- A dynamic, in-memory data structure where multiple instances can remain active during program execution. The memtable size is configurable, allowing for adjustment based on workload and resource constraints.  
  - When one memtable instance reaches its capacity, its data is flushed to disk to maintain performance.  
  - A second memtable instance seamlessly takes over while the first is being written to disk, ensuring continuous operation without interruptions.  
  - The SSParser manages memtable instances and transforms their data into the SSTable format. It calculates data chunk offsets and generates index and summary tables to organize the flushed data efficiently.  

- **SSTable**:    
*The SSTable is a disk-based, structured data format used for storage and retrieval. It supports two configurable approaches:*    
  - A single-file SSTable that encapsulates all components, or  
  - A multi-file SSTable where each component, such as metadata or indexes, is stored separately.  

 - *The SSTable is composed of key parts including:*      

    - **Metadata**: Provides contextual details about the SSTable.  

    - **Filter**: Helps determine the likelihood of a key's existence in the table.  

    - **Merkle Tree**: Ensures data consistency and integrity.  

    - **Summary and Index**: Facilitate efficient key lookups.  

    - **Data**: Stores the actual key-value pairs.  

- **Block Manager**:  
    This component manages smooth data writes and reads using fixed-size blocks (configurable for specific needs). It works alongside file writers and readers, enabling reusable and modular block management across the project. This design reduces tight coupling and minimizes unnecessary reads of non-essential data.
 
  ### **Read Path** 📖: 
  For executing search queries and returning results.
 
   ![read path](/assets/read%20path.png)
 
   **Cache Layer**: For optimizing frequently accessed data. 
       - Block cache: component relying on LRU algorithm. Consisting of a doubly linked list storing actual block data and a hash map storing key-value pairs [block id , file name] : data_pointer. This aproach allows our system to have constant cache access time.  

   **Bloom Filter**: For optimizing data lookups. If the key is not present in the bloom filter, we continue the lookup in other sstable files. Loaded into memory.
  
   **SStable Summary**: For optimizing data lookups. In the isection of sorted keys we choose ranges that are present in the summary. Loaded into memory. 

   **SStable Index**: After getting the valid key offset range, Index leads us to value of the actual data chunk offset.

   **SStable Data**: Resembles the last detionation in our read path, stores actual data bytes.
 
 ---
 
 ## Data Storage
 
 Data is managed through an efficient combination of in-memory and disk-based storage:
 
 ### Write-Ahead Log (WAL): Ensures durability by recording operations before applying them.
 - **WAL** resembles the staple of stability in the system. By using WAL we can track the trace of our system operations running even when the system expiriences an unexpected crash.
 
 ![wal structure](/assets/wal.png)
 
 ### Memtable: Temporarily holds records in memory for fast writes and retrievals.
 
 ### SSTable: Stores data persistently on disk, organized into levels for efficient compaction and retrieval.
   - Current version does not support lsm tree optimization (organization into levels).
 
 
 ![index](/assets/index.png)
 
### Important Information about SSTable Structure during Read Operations

#### During read operations in an SSTable (Sorted Strings Table), specific components are loaded into memory to optimize access and reduce disk I/O. Here's how the process works:

* In-Memory Components:  
        **Summary**: This contains a condensed mapping of keys to offsets in the Index file. It allows the system to quickly locate the approximate position of a key in the Index, significantly reducing the number of disk seeks required.  
        **Metadata**: This includes important information about the SSTable, such as its generation, compression type, and other configuration details. Metadata helps in managing and interpreting the SSTable.  
        **Filter (e.g., Bloom Filter)**: This probabilistic data structure quickly determines if a key might exist in the SSTable, allowing the system to avoid unnecessary disk lookups for keys that are definitely not present.  
        **Merkle Tree**: This data structure is used for efficient validation and consistency checks, particularly in distributed systems. It ensures data integrity and helps in identifying inconsistencies.  

* On-Disk Components:  
        **Index**: The Index maps every key to its corresponding location in the Data file. It is accessed based on offsets calculated from the Summary.  
        **Data**: This is where the actual key-value pairs are stored. Once the Index provides the exact location, the Data file is read to retrieve the required information.  

* Access Process:  
        *The read operation begins with the Filter, which quickly determines if the requested key might exist in the SSTable.*  
        *If the Filter indicates a possible match, the Summary is consulted to locate the approximate position of the key in the Index.*  
        *The exact offset of the key in the Data file is retrieved from the Index, allowing direct access to the required data without scanning the entire file.*  
        **This multi-layered structure minimizes the number of disk accesses, significantly enhancing read performance.**


 ---

 ## Configuration

🔧[Not in use] To configure the NoSQL engine, modify the config.json file located in the src/config directory of the project. The configuration file allows you to tailor the engine to specific use cases and workloads.
 
 ---

 ## License

 This project is licensed under the MIT License. You are free to use, modify, and distribute this software under the terms of the MIT License.