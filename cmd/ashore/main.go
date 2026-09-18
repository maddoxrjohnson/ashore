package main

import (
	"flag"
	"fmt"
	"os"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("ashore", version)
		return
	}
	fmt.Fprintln(os.Stderr, "usage: ashore --version")
	os.Exit(2)
}
