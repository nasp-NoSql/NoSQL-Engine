package main

import (
	"bufio"
	"fmt"
	"nosqlEngine/src/engine"
	"os"
	"strings"
)

func main() {
	fmt.Println("=== NoSQL Engine ===")
	fmt.Println("Starting engine...")

	// Create a new engine instance
	eng := engine.NewEngine()
	if eng == nil {
		fmt.Println("Failed to create engine. Exiting...")
		return
	}

	// Start the engine
	eng.Start()
	defer func() {
		if err := eng.Close(); err != nil {
			fmt.Printf("Error closing engine: %v\n", err)
		}
	}()

	fmt.Println("Engine started successfully!")
	fmt.Println("Available commands: put, get, delete, quit")
	fmt.Println("Type 'help' for more information")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("\n> ")
		if !scanner.Scan() {
			break
		}

		command := strings.TrimSpace(strings.ToLower(scanner.Text()))

		switch command {
		case "put":
			handlePut(scanner, eng)
		case "get":
			handleGet(scanner, eng)
		case "delete":
			handleDelete(scanner, eng)
		case "help":
			showHelp()
		case "quit", "exit", "q":
			fmt.Println("Goodbye!")
			return
		case "":
			// Empty input, continue
			continue
		default:
			fmt.Printf("Unknown command: %s\n", command)
			fmt.Println("Type 'help' for available commands")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}

func handlePut(scanner *bufio.Scanner, eng *engine.Engine) {
	// Get key
	fmt.Print("Enter key: ")
	if !scanner.Scan() {
		fmt.Println("Error reading key")
		return
	}
	key := strings.TrimSpace(scanner.Text())
	if key == "" {
		fmt.Println("Key cannot be empty")
		return
	}

	// Get value
	fmt.Print("Enter value: ")
	if !scanner.Scan() {
		fmt.Println("Error reading value")
		return
	}
	value := strings.TrimSpace(scanner.Text())
	if value == "" {
		fmt.Println("Value cannot be empty")
		return
	}

	// TODO: Call engine.Put(key, value) when the method is implemented
	fmt.Printf("PUT operation: key='%s', value='%s'\n", key, value)
	fmt.Println("Note: Engine.Put() method not yet implemented")
}

func handleGet(scanner *bufio.Scanner, eng *engine.Engine) {
	// Get key
	fmt.Print("Enter key: ")
	if !scanner.Scan() {
		fmt.Println("Error reading key")
		return
	}
	key := strings.TrimSpace(scanner.Text())
	if key == "" {
		fmt.Println("Key cannot be empty")
		return
	}

	// TODO: Call engine.Get(key) when the method is implemented
	fmt.Printf("GET operation: key='%s'\n", key)
	fmt.Println("Note: Engine.Get() method not yet implemented")
}

func handleDelete(scanner *bufio.Scanner, eng *engine.Engine) {
	// Get key
	fmt.Print("Enter key: ")
	if !scanner.Scan() {
		fmt.Println("Error reading key")
		return
	}
	key := strings.TrimSpace(scanner.Text())
	if key == "" {
		fmt.Println("Key cannot be empty")
		return
	}

	// TODO: Call engine.Delete(key) when the method is implemented
	fmt.Printf("DELETE operation: key='%s'\n", key)
	fmt.Println("Note: Engine.Delete() method not yet implemented")
}

func showHelp() {
	fmt.Println("\nAvailable commands:")
	fmt.Println("  put    - Store a key-value pair")
	fmt.Println("  get    - Retrieve value by key")
	fmt.Println("  delete - Remove a key-value pair")
	fmt.Println("  help   - Show this help message")
	fmt.Println("  quit   - Exit the application")
	fmt.Println("\nUsage:")
	fmt.Println("  1. Type a command and press Enter")
	fmt.Println("  2. Follow the prompts to enter key/value")
	fmt.Println("  3. Use 'quit' to exit")
}
