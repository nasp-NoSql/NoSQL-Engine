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

func main() {
<<<<<<< HEAD
=======
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
	case "EXIT", "QUIT", "Q":
		fmt.Printf("%s[INFO]%s Shutting down engine...\n", ColorCyan, ColorReset)
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
	user := "default" // Default user for CLI
	
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
	} else if found {
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

func clearScreen() {
	fmt.Print("\033[2J\033[H")
	printWelcome()
>>>>>>> cc3f9be0683bf27eb59232a026a72adbb35af83e
}
