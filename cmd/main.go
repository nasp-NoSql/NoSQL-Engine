package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"nosqlEngine/src/config"
	"nosqlEngine/src/engine"
)

const (
	// ANSI Color codes for beautiful output
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorPurple = "\033[35m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
	ColorBold   = "\033[1m"
)
var CONFIG = config.GetConfig()

func main() {
	printWelcome()

	// Initialize and start the engine
	fmt.Printf("%s[INFO]%s Starting NoSQL Engine...\n", ColorCyan, ColorReset)
	eng := engine.NewEngine()
	eng.Start()
	fmt.Printf("%s[SUCCESS]%s Engine started successfully!\n", ColorGreen, ColorReset)

	// Create scanner for user input
	scanner := bufio.NewScanner(os.Stdin)

	printHelp()

	for {
		printPrompt()

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		handleCommand(eng, input)
	}

	fmt.Printf("\n%s[INFO]%s Goodbye! 👋\n", ColorCyan, ColorReset)
}

func printWelcome() {
	fmt.Printf("%s%s", ColorBold, ColorBlue)
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    🚀 NoSQL Engine CLI 🚀                    ║")
	fmt.Println("║                                                              ║")
	fmt.Println("║              High-Performance Key-Value Store               ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Printf("%s", ColorReset)
	fmt.Println()
}

func printHelp() {
	fmt.Printf("%s%sAvailable Commands:%s\n", ColorBold, ColorYellow, ColorReset)
	fmt.Printf("  %s📝 PUT <key> <value>%s    - Store a key-value pair\n", ColorGreen, ColorReset)
	fmt.Printf("  %s🔍 GET <key>%s           - Retrieve value for a key\n", ColorBlue, ColorReset)
	fmt.Printf("  %s🗑️  DELETE <key>%s        - Delete a key-value pair\n", ColorRed, ColorReset)
	fmt.Printf("  %s📊 STATS%s              - Show engine statistics\n", ColorPurple, ColorReset)
	fmt.Printf("  %s❓ HELP%s               - Show this help message\n", ColorCyan, ColorReset)
	fmt.Printf("  %s🔍 PREFIX_SCAN <prefix> <pageNum> <pageSize>%s - Use prefix scan\n", ColorWhite, ColorReset)
	fmt.Printf("  %s🔄 PREFIX_ITERATE <prefix>%s - Use prefix iterator\n", ColorWhite, ColorReset)
	fmt.Printf("  %s🚪 EXIT%s               - Exit the application\n", ColorYellow, ColorReset)
	fmt.Println()
}

func printPrompt() {
	fmt.Printf("%s%sNoSQL>%s ", ColorBold, ColorGreen, ColorReset)
}

func handleCommand(eng *engine.Engine, input string) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	command := strings.ToUpper(parts[0])

	switch command {
	case "PUT":
		handlePut(eng, parts)
	case "GET":
		handleGet(eng, parts)
	case "DELETE", "DEL":
		handleDelete(eng, parts)
	case "STATS":
		handleStats(eng)
	case "HELP", "H":
		printHelp()
	case "PREFIX_ITERATE":
		handlePrefixIterator(eng, parts)
	case "EXIT", "QUIT", "Q":
		fmt.Printf("%s[INFO]%s Shutting down engine...\n", ColorCyan, ColorReset)
		eng.Shut()
		os.Exit(0)
	case "CLEAR", "CLS":
		clearScreen()
	default:
		fmt.Printf("%s[ERROR]%s Unknown command: %s\n", ColorRed, ColorReset, command)
		fmt.Printf("Type %sHELP%s for available commands.\n", ColorCyan, ColorReset)
	}
}

func handlePut(eng *engine.Engine, parts []string) {
	if len(parts) < 3 {
		fmt.Printf("%s[ERROR]%s Usage: PUT <key> <value>\n", ColorRed, ColorReset)
		return
	}

	key := parts[1]
	value := strings.Join(parts[2:], " ") // Allow spaces in values
	user := "default"                     // Default user for CLI

	start := time.Now()
	err := eng.Write(user, key, value, false)
	duration := time.Since(start)

	if err == nil {
		fmt.Printf("%s[SUCCESS]%s ✅ PUT '%s' -> '%s' %s(%.2fms)%s\n",
			ColorGreen, ColorReset, key, value, ColorYellow, float64(duration.Nanoseconds())/1e6, ColorReset)
	} else {
		fmt.Printf("%s[ERROR]%s ❌ Failed to store key '%s': %v\n", ColorRed, ColorReset, key, err)
	}
}

func handleGet(eng *engine.Engine, parts []string) {
	if len(parts) != 2 {
		fmt.Printf("%s[ERROR]%s Usage: GET <key>\n", ColorRed, ColorReset)
		return
	}

	key := parts[1]
	user := "default" // Default user for CLI

	start := time.Now()
	value, found, err := eng.Read(user, key)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("%s[ERROR]%s ❌ Error reading key '%s': %v\n", ColorRed, ColorReset, key, err)
	} else if found && value != CONFIG.Tombstone {
		fmt.Printf("%s[SUCCESS]%s 🔍 GET '%s' -> '%s' %s(%.2fms)%s\n",
			ColorGreen, ColorReset, key, value, ColorYellow, float64(duration.Nanoseconds())/1e6, ColorReset)
	} else {
		fmt.Printf("%s[NOT FOUND]%s 🚫 Key '%s' not found %s(%.2fms)%s\n",
			ColorYellow, ColorReset, key, ColorYellow, float64(duration.Nanoseconds())/1e6, ColorReset)
	}
}

func handleDelete(eng *engine.Engine, parts []string) {
	if len(parts) != 2 {
		fmt.Printf("%s[ERROR]%s Usage: DELETE <key>\n", ColorRed, ColorReset)
		return
	}

	key := parts[1]
	user := "default" // Default user for CLI

	// Get the tombstone value from config
	cfg := config.GetConfig()
	tombstone := cfg.Tombstone

	start := time.Now()
	err := eng.Write(user, key, tombstone, false) // Delete by writing tombstone value
	duration := time.Since(start)

	if err == nil {
		fmt.Printf("%s[SUCCESS]%s 🗑️ DELETE '%s' %s(%.2fms)%s\n",
			ColorGreen, ColorReset, key, ColorYellow, float64(duration.Nanoseconds())/1e6, ColorReset)
	} else {
		fmt.Printf("%s[ERROR]%s ❌ Failed to delete key '%s': %v\n", ColorRed, ColorReset, key, err)
	}
}

func handleStats(eng *engine.Engine) {
	fmt.Printf("%s%s📊 Engine Statistics:%s\n", ColorBold, ColorPurple, ColorReset)
	fmt.Printf("  %s├─%s Status: %sRunning%s\n", ColorPurple, ColorReset, ColorGreen, ColorReset)
	fmt.Printf("  %s├─%s Engine: %sActive%s\n", ColorPurple, ColorReset, ColorCyan, ColorReset)
	fmt.Printf("  %s└─%s Version: %s1.0.0%s\n", ColorPurple, ColorReset, ColorBlue, ColorReset)
	// Add more statistics as needed based on your engine implementation
	_ = eng // Prevent unused parameter warning
}

func handlePrefixIterator(eng *engine.Engine, parts []string) {
	user := "default"
	prefix := parts[1]
	iterator, err := eng.PrefixIterate(user, prefix)
	if err != nil {
		fmt.Printf("%s[ERROR]%s ❌ Error creating prefix iterator: %v\n", ColorRed, ColorReset, err)
		return
	}

	fmt.Printf("%s[SUCCESS]%s 🔄 Prefix iterator created for prefix '%s%s%s'. Use 'next' to get next record, 'stop' to terminate.\n", 
		ColorGreen, ColorReset, ColorCyan, prefix, ColorReset)

	for {
		var command string
		fmt.Printf("%s🔄 Iterator>%s ", ColorBlue, ColorReset)
		fmt.Scanln(&command)

		switch command {
		case "next":
			key, value, hasNext := iterator.Next()
			if key == "" && value == "" {
				fmt.Printf("%s[INFO]%s 🔚 No more records.\n", ColorYellow, ColorReset)
				return
			}
			fmt.Printf("%s[RECORD]%s 📋 Key: %s%s%s, Value: %s%s%s\n", 
				ColorGreen, ColorReset, ColorCyan, key, ColorReset, ColorBlue, value, ColorReset)
			if !hasNext {
				fmt.Printf("%s[INFO]%s ✅ This was the last record.\n", ColorYellow, ColorReset)
				return
			}
		case "stop":
			iterator.Stop()
			fmt.Printf("%s[INFO]%s 🛑 Iterator stopped.\n", ColorYellow, ColorReset)
			return
		case "has_next":
			if iterator.HasNext() {
				fmt.Printf("%s[INFO]%s ✅ Iterator has more records.\n", ColorGreen, ColorReset)
			} else {
				fmt.Printf("%s[INFO]%s ❌ Iterator has no more records.\n", ColorYellow, ColorReset)
			}
		case "reset":
			iterator.Reset()
			fmt.Printf("%s[INFO]%s 🔄 Iterator reset to beginning.\n", ColorGreen, ColorReset)
		default:
			fmt.Printf("%s[ERROR]%s ❓ Unknown command. Available commands: %snext%s, %sstop%s, %shas_next%s, %sreset%s\n", 
				ColorRed, ColorReset, ColorCyan, ColorReset, ColorCyan, ColorReset, ColorCyan, ColorReset, ColorCyan, ColorReset)
		}
	}
}

func clearScreen() {
	fmt.Print("\033[2J\033[H")
	printWelcome()
}
