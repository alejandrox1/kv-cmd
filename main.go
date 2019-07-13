/*
Simple command line REPL that drives a simple in-memory key/value storage system.
This system should allow for nested transactions.
A transaction can be commited or aborted.


  Implementation
  ---------------
* All keys and values are ASCII strings delimited by whitespaces. No quoting
  needed.
* All keys and values are stored as strings.
* Errors are output to stderr.
* Commands are case-insensitive (i.e., READ == read).
*/
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var store map[string]string

const (
	PROMPT = "> "

	// Commands.
	READ   = "READ"   // key
	WRITE  = "WRITE"  // key value
	DELETE = "DELETE" // key

	QUIT = "QUIT"

	START  = "START"
	COMMIT = "COMMIT"
	ABORT  = "ABORT"

	// Usage message for this program.
	USAGE = `

    Available commands:
    -------------------
    READ <key>           Print value of <key>
    WRITE <key> <value>  Store <value> in <key>
    DELETE <key>         Delete <key>

    START                Start a transaction
    COMMIT               Commit transaction
    ABORT                Abort transaction

    QUIT                 Exit program
    `
)

// exitLog logs the string err message to stderr and exits with error code 1.
func exitLog(err string) {
	log(err)
	os.Exit(1)
}

// log logs the string err message to stderr.
func log(err string) {
	fmt.Fprintln(os.Stderr, err)
}

// preProcessInput checks that there are more than one but less than three
// words and returns an error if either of these two conditions are not true
// else, return each word individually.
func preProcessInput(words []string) (string, string, string, error) {
	var cmd, key, value string

	if len(words) < 1 {
		return cmd, key, value, fmt.Errorf("Error: expected at least one command: %s", USAGE)
	}
	if len(words) > 3 {
		return cmd, key, value, fmt.Errorf("Error: too many arguments: %s", USAGE)
	}

	cmd = strings.ToUpper(words[0])
	if len(words) > 1 {
		key = words[1]
	}
	if len(words) > 2 {
		value = words[2]
	}

	return cmd, key, value, nil
}

func parseTransaction(kvStore map[string]string) map[string]string {
	tranStore := make(map[string]string)
	for k, v := range kvStore {
		tranStore[k] = v
	}

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(PROMPT)
		scanned := scanner.Scan()
		if !scanned {
			exitLog(fmt.Sprintf("Error reading standard input: %s", scanner.Err()))
		}

		words := strings.Fields(scanner.Text())
		cmd, key, value, err := preProcessInput(words)
		if err != nil {
			log(err.Error())
			continue
		}

		switch cmd {
		case READ:
			if value, ok := tranStore[key]; ok {
				fmt.Println(value)
			} else {
				log(fmt.Sprintf("Key not found: %s", key))
			}
		case WRITE:
			tranStore[key] = value
		case DELETE:
			if _, ok := tranStore[key]; ok {
				delete(tranStore, key)
			} else {
				log(fmt.Sprintf("Key not found: %s", key))
			}
		case QUIT:
			fmt.Println("Exiting...")
			os.Exit(0)
		case START:
			transaction := parseTransaction(tranStore)
			// If transaction was not aborted...
			if transaction != nil {
				// Synchronize the contents of the store with those of the
				// transaction.
				tranStore = transaction
			}
		case COMMIT:
			return tranStore
		case ABORT:
			return nil
		default:
			log(fmt.Sprintf("Unrecognized command: %s", cmd))
		}
	}

	return tranStore
}

func parentTransaction() {
	// Initialize empty store.
	store = make(map[string]string)

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(PROMPT)
		scanned := scanner.Scan()
		if !scanned {
			exitLog(fmt.Sprintf("Error reading standard input: %s", scanner.Err()))
		}

		words := strings.Fields(scanner.Text())
		cmd, key, value, err := preProcessInput(words)
		if err != nil {
			log(err.Error())
			continue
		}

		switch cmd {
		case READ:
			if value, ok := store[key]; ok {
				fmt.Println(value)
			} else {
				log(fmt.Sprintf("Key not found: %s", key))
			}
		case WRITE:
			store[key] = value
		case DELETE:
			if _, ok := store[key]; ok {
				delete(store, key)
			} else {
				log(fmt.Sprintf("Key not found: %s", key))
			}
		case QUIT:
			fmt.Println("Exiting...")
			os.Exit(0)
		case START:
			transaction := parseTransaction(store)
			// If transaction was not aborted...
			if transaction != nil {
				// Synchronize the contents of the store with those of the
				// transaction.
				store = transaction
			}
		case COMMIT:
			log("Error: you are not currently in a transaction")
		case ABORT:
			log("Error: you are not currently in a transaction")
		default:
			log(fmt.Sprintf("Unrecognized command: %s", cmd))
		}
	}
}

func main() {
	parentTransaction()
}
