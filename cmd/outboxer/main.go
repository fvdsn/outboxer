package main

import (
	"context"
	"os"

	"outboxer-go/internal/outboxer"
)

func main() {
	outboxer.Run(context.Background(), os.Args[1:])
}
