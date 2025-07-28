# 🚀 NoSQL Engine CLI

A beautiful command-line interface for your high-performance NoSQL key-value store.

## ✨ Features

- **🎨 Beautiful colored output** with emojis and formatting
- **⚡ Performance timing** for all operations
- **🛡️ Error handling** with detailed messages
- **📊 Statistics** and monitoring
- **🔄 User-friendly commands** with help system

## 🏃‍♂️ Quick Start

### Build and Run
```bash
# Build the engine
go build -o bin/nosql-engine cmd/main.go

# Run the CLI
./bin/nosql-engine
```

## 📝 Available Commands

### Core Operations

#### PUT - Store Key-Value Pairs
```
PUT <key> <value>
```
**Examples:**
```
PUT name "John Doe"
PUT age 25
PUT city "New York"
PUT description "This is a long description with spaces"
```

#### GET - Retrieve Values
```
GET <key>
```
**Examples:**
```
GET name
GET age
GET city
```

#### DELETE - Remove Keys
```
DELETE <key>
```
**Examples:**
```
DELETE name
DELETE age
```

### Utility Commands

#### STATS - Engine Statistics
```
STATS
```
Shows current engine status and performance metrics.

#### HELP - Show Help
```
HELP
```
Displays all available commands and usage instructions.

#### EXIT - Quit Application
```
EXIT
```
or
```
QUIT
```
or
```
Q
```

#### CLEAR - Clear Screen
```
CLEAR
```
or
```
CLS
```

## 🎯 Usage Examples

### Interactive Session Example
```
╔══════════════════════════════════════════════════════════════╗
║                    🚀 NoSQL Engine CLI 🚀                    ║
║                                                              ║
║              High-Performance Key-Value Store               ║
╚══════════════════════════════════════════════════════════════╝

[INFO] Starting NoSQL Engine...
[SUCCESS] Engine started successfully!

Available Commands:
  📝 PUT <key> <value>    - Store a key-value pair
  🔍 GET <key>           - Retrieve value for a key
  🗑️  DELETE <key>        - Delete a key-value pair
  📊 STATS              - Show engine statistics
  ❓ HELP               - Show this help message
  🚪 EXIT               - Exit the application

NoSQL> PUT user:1 "Alice Johnson"
[SUCCESS] ✅ PUT 'user:1' -> 'Alice Johnson' (0.15ms)

NoSQL> PUT user:2 "Bob Smith"  
[SUCCESS] ✅ PUT 'user:2' -> 'Bob Smith' (0.12ms)

NoSQL> GET user:1
[SUCCESS] 🔍 GET 'user:1' -> 'Alice Johnson' (0.08ms)

NoSQL> GET user:3
[NOT FOUND] 🚫 Key 'user:3' not found (0.05ms)

NoSQL> DELETE user:2
[SUCCESS] 🗑️ DELETE 'user:2' (0.10ms)

NoSQL> STATS
📊 Engine Statistics:
  ├─ Status: Running
  ├─ Engine: Active
  └─ Version: 1.0.0

NoSQL> EXIT
[INFO] Goodbye! 👋
```

## 🎨 Color Coding

- **🟢 Green** - Success messages and prompts
- **🔴 Red** - Error messages
- **🟡 Yellow** - Warnings and timing info
- **🔵 Blue** - Informational messages
- **🟣 Purple** - Statistics and special info
- **🔷 Cyan** - System messages

## ⚡ Performance Features

- **Real-time timing** - See how fast each operation executes
- **Memory efficient** - Optimized for large datasets
- **Concurrent safe** - Thread-safe operations
- **WAL support** - Write-ahead logging for durability

## 🛠️ Technical Details

- **Engine**: Custom LSM-Tree based storage engine
- **Persistence**: Write-Ahead Logging (WAL) + SSTable compaction
- **In-Memory**: Multiple memtables for high write throughput
- **Bloom Filters**: Fast negative lookups
- **Rate Limiting**: Built-in user rate limiting

## 🚀 Tips for Best Performance

1. **Batch operations** when possible
2. **Use meaningful keys** for better organization
3. **Monitor stats** regularly with `STATS` command
4. **Keys are case-sensitive** - "Key" ≠ "key"
5. **Values can contain spaces** - use quotes for multi-word values

## 🐛 Troubleshooting

### Common Issues

**Rate Limiting Error:**
```
[ERROR] user default is not allowed to write: rate limit exceeded
```
*Solution: Wait a moment and try again, or check token bucket settings*

**Key Not Found:**
```
[NOT FOUND] 🚫 Key 'mykey' not found
```
*Solution: Verify the key exists with correct spelling and case*

### Getting Help

If you encounter issues:
1. Type `HELP` for command reference
2. Check your key spelling and case
3. Verify the engine started successfully
4. Look for error messages in red

---

**Happy querying!** 🎉
