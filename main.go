package main

import (
	"log"

	"github.com/tomassar/distributed-fs-go/p2p"
)

func main() {

	tr := p2p.NewTCPTransport(":3000")
	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	select {}
}
