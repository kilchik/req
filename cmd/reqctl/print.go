package main

import "fmt"

const (
	colorRed    = "\u001b[31m"
	colorGreen  = "\u001b[32m"
	colorYellow = "\u001b[33m"
	colorReset  = "\u001b[0m"
)

func printColorized(color string, format string, args ...interface{}) {
	fmt.Print(color)
	fmt.Printf(format, args...)
	fmt.Print(colorReset)
	fmt.Println()
}

func printError(format string, args ...interface{}) {
	printColorized(colorRed, format, args...)
}

func printSuccess(format string, args ...interface{}) {
	printColorized(colorGreen, format, args...)
}

func printPrompt() {
	fmt.Printf("> ")
}
