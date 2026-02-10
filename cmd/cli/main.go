package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"HM2/internal/cli"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	st := cli.NewState()

	in := bufio.NewScanner(os.Stdin)
	fmt.Println("hm2-cli (type 'help' or 'exit')")
	for {
		fmt.Print("> ")
		if !in.Scan() {
			return
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		if line == "exit" {
			return
		}
		if err := cli.ExecuteLine(st, line, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}
}
