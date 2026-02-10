package main

import (
	"flag"

	"HM2/internal/node"
)

func main() {
	ID := flag.String("id", "A", "Node ID")
	host := flag.String("host", "127.0.0.1", "Node hostname")
	port := flag.Int("port", 8081, "Node port")

	flag.Parse()

	peerNode := node.NewNode(*ID, *host, *port)
	err := peerNode.Start()

	if err != nil {
		panic(err)
	}
}
