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
- [How It Works](#how-it-works)  
  - [Architecture Overview](#architecture-overview)  
  - [Data Storage](#data-storage)  
  - [Indexing](#indexing)  
  - [Querying](#querying)  
  - [Consistency](#consistency)  
- [Configuration](#configuration)  
- [Contributing](#contributing)  
- [License](#license)  

---

## Introduction  
**NoSQL Engine** is a lightweight, efficient, and customizable NoSQL database engine written in Go. Designed for speed and flexibility, it is ideal for scenarios requiring immutable data storage and fast lookups. Our NoSQL engine draws inspiration from modern NoSQL technologies, including Apache Cassandra, DynamoDB, and similar distributed systems.

---

## Features

### Available Features
- **SSTable-Based Storage**: Efficient immutable storage for data.
- **Write-Ahead Logging (WAL)**: Protects data integrity by logging changes before committing them to disk.
- **Bloom Filter for Fast Lookups**: Reduces unnecessary disk reads by filtering out non-matching entries.
- **Merkle Tree for SSTables**: Ensures data integrity by detecting and resolving data corruption.
- **Custom Indexing**: Enables fast and efficient key-value retrieval with an optimized index structure.
- **Data Caching**: In-memory caching for frequently accessed data to improve read performance.

---

### Coming Soon 🚀
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

### Steps to Install

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

 # How it works
 
 ## Architecture Overview
 
 The search engine is built around a modular architecture that separates concerns and ensures scalability. It consists of:
 
   **Write Path** : For ingesting data efficiently and preparing it for search.      
 
 ![write path](image.png)
 
   - Memtable : multiple in-memory instances active during program operations. Configuration allowing memtable size change. When an instance of memtable fills with data, data gets flushed to the disk. Apearance of the second instance allowing the engine to operate smoothly while the data is being written to the disk.
       - SSparser performing the memtable instance management as well as transforming memtable data to a valid sstable format. Calclating data chunk offsets and forming index summary tables.
 
   - SStable : data structured on the disk. Configuration allowing 2 types of aproach. Single file sstable or a multiple file sstable. Consisting of metadata, filter, merkle tree, summary, index and data parts. 
       - Block manager allows smooth data writes/reads in fixed size blocks (possible configuration). Using file writers and readers on top of an instance of block manager we allow for block manager component to be recycled through the project while eliminating tight coupling, and reducing reads of non-important data.
 
   **Read Path**: For executing search queries and returning results.
 
   ![read path](image-1.png)
 
   **Cache Layer**: For optimizing frequently accessed data. 
       - Block cache: comopnent relying on LRU algorithm. Consisting of a doubly linked list storing actual block data and a hash map storing key-value pairs [block id , file name] : data_pointer. This aproach allows our system to have constant cache access time.
   **Bloom Filter**: For optimizing data lookups. If the key is not present in the bloom filter, we continue the lookup in other sstable files. Loaded into memory.
 
   **SStable Summary**: For optimizing data lookups. In the isection of sorted keys we choose ranges that are present in the summary. Loaded into memory. 
 
   
 
 ## Data Storage
 
 Data is managed through an efficient combination of in-memory and disk-based storage:
 
 ### Write-Ahead Log (WAL): Ensures durability by recording operations before applying them.
 - **WAL** resembles the staple of stability in the system. By using WAL we can track the trace of our system operations running even when the system expiriences an unexpected crash.
 
 ![wal structure](image-2.png)
 
 ### Memtable: Temporarily holds records in memory for fast writes and retrievals.
 
 ### SSTable: Stores data persistently on disk, organized into levels for efficient compaction and retrieval.
   - Current version does not support lsm tree optimization (organization into levels).
 
 
 ![index](image-3.png)
 
 Indexing is at the core of the search engine's functionality.
 
 
 This structure is clear and matches the requested template. It provides a thorough explanation of the search engine's workflow and underlying mechanisms. Let me know if additional details or modifications are needed!