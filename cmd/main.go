package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"nosqlEngine/src/config"
	"nosqlEngine/src/engine"

	"nosqlEngine/src/models/countmin_sketch"
	"nosqlEngine/src/models/hyperloglog"
	"nosqlEngine/src/models/simhash"
)

func getSerializedPath(filename string) string {
	wd, err := os.Getwd()
	if err != nil {
		panic("Cannot get working directory")
	}
	return filepath.Join(wd, "src/serialized", filename)
}

var CMS *countmin_sketch.CountMinSketch
var HLL *hyperloglog.HyperLogLog
var SH *simhash.SimHash

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
	printWelcome()

	// Initialize and start the engine
	fmt.Printf("%s[INFO]%s Starting NoSQL Engine...\n", ColorCyan, ColorReset)
	eng := engine.NewEngine()
	eng.Start()

	initSketches()

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

func initSketches() {
	// Try loading CMS
	fmt.Println("[DEBUG] Trying to load CMS from:", getSerializedPath(""))
	cms, err := countmin_sketch.Deserialize(getSerializedPath("cms.bin"))
	if err != nil {
		fmt.Print("Errorcms")
		CMS = &countmin_sketch.CountMinSketch{}
		CMS.Initialize(0.01, 0.001)
	} else {
		CMS = cms
	}

	// Try loading HLL
	hll, err := hyperloglog.Deserialize(getSerializedPath("hll.bin"))
	if err != nil {
		fmt.Print("Errorhll")
		HLL = &hyperloglog.HyperLogLog{}
		HLL.Initialize(0.01)
	} else {
		HLL = &hll
	}

	// Try loading SH
	sh, err := simhash.Deserialize(getSerializedPath("sh.bin"))
	if err != nil {
		fmt.Print("Errorsh")
		SH = &simhash.SimHash{}
	} else {
		SH = &sh
	}
}

func saveSketches() {
	CMS.Serialize(getSerializedPath("cms.bin"))
	HLL.Serialize(getSerializedPath("hll.bin"))
	SH.Serialize(getSerializedPath("sh.bin"))
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
	fmt.Printf("  %s📊 FREQ%s              - Shows the frequency of a key\n", ColorGreen, ColorReset)
	fmt.Printf("  %s📊 HDIST%s              - Ham. Dist. between two keys\n", ColorGreen, ColorReset)
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
		saveSketches()
		os.Exit(0)
	case "CLEAR", "CLS":
		clearScreen()
	case "FREQ":
		handleFrequency(parts)
	case "HDIST":
		if len(parts) != 3 {
			fmt.Printf("%s[ERROR]%s Usage: HDISTKEY <key1> <key2>\n", ColorRed, ColorReset)
			return
		}
		handleHammingDistanceFromKeys(eng, parts[1], parts[2])
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

	CMS.Add([]byte(key))
	HLL.Add([]byte(key))

	SH.Generate(strings.Split(value, " "))
	fingerprint := SH.Hash

	start := time.Now()
	err := eng.Write(user, key, value, false)
	duration := time.Since(start)

	if err == nil {
		fmt.Printf("%s[SUCCESS]%s ✅ PUT '%s' -> '%s' %s(%.2fms)%s\n",
			ColorGreen, ColorReset, key, value, ColorYellow, float64(duration.Nanoseconds())/1e6, ColorReset)

		fmt.Print("Fingerprint: ", fingerprint, "\n")
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

	CMS.Add([]byte(key))
	HLL.Add([]byte(key))

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

	CMS.Add([]byte(key))
	HLL.Add([]byte(key))

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

func handleFrequency(parts []string) {
	if len(parts) != 2 {
		fmt.Printf("%s[ERROR]%s Usage: HOTKEY <key>\n", ColorRed, ColorReset)
		return
	}

	key := parts[1]
	count := CMS.Estimate([]byte(key))
	fmt.Printf("%s[INFO]%s Estimated frequency of key '%s': %d\n", ColorCyan, ColorReset, key, count)
}

func handleResetSketches() {
	CMS.Initialize(0.01, 0.001)
	fmt.Println("[INFO] CMS reset successfully.")

	HLL.Initialize(0.01)
	fmt.Println("[INFO] HLL reset successfully.")
}

func handleStats(eng *engine.Engine) {
	fmt.Printf("%s%s📊 Engine Statistics:%s\n", ColorBold, ColorPurple, ColorReset)
	fmt.Printf("  %s├─%s Status: %sRunning%s\n", ColorPurple, ColorReset, ColorGreen, ColorReset)
	fmt.Printf("  %s├─%s Engine: %sActive%s\n", ColorPurple, ColorReset, ColorCyan, ColorReset)
	fmt.Printf("  %s├─%s Version: %s1.0.0%s\n", ColorPurple, ColorReset, ColorBlue, ColorReset)
	fmt.Printf("  %s└─%s HLL Unique Estimate: %d%s\n", ColorPurple, ColorReset, HLL.Estimate(), ColorReset)
	// Add more statistics as needed based on your engine implementation
	_ = eng // Prevent unused parameter warning
}

func handleHammingDistanceFromKeys(eng *engine.Engine, key1, key2 string) {
	user := "default"

	val1, found1, err1 := eng.Read(user, key1)
	val2, found2, err2 := eng.Read(user, key2)

	if err1 != nil || err2 != nil {
		fmt.Printf("%s[ERROR]%s Failed to read one of the keys: %v %v\n", ColorRed, ColorReset, err1, err2)
		return
	}

	if !found1 || !found2 {
		fmt.Printf("%s[NOT FOUND]%s One or both keys not found: '%s', '%s'\n", ColorYellow, ColorReset, key1, key2)
		return
	}

	sh1 := simhash.SimHash{}
	sh2 := simhash.SimHash{}

	sh1.Generate(strings.Split(val1, " "))
	sh2.Generate(strings.Split(val2, " "))

	dist := simhash.HammingDistance(sh1.Hash, sh2.Hash)

	fmt.Printf("%s[INFO]%s Hamming distance between values of '%s' and '%s': %d\n",
		ColorCyan, ColorReset, key1, key2, dist)
}

func clearScreen() {
	fmt.Print("\033[2J\033[H")
	printWelcome()
}
