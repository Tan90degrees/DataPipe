package main

import (
	"fmt"
	"os"

	"datapipe/cmd/cli/commands"
)

var version = "1.0.0"

func main() {
	rootCmd := commands.NewRootCmd(version)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
