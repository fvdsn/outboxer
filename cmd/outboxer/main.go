package main

import (
	"context"
	"os"

	"github.com/fvdsn/outboxer/internal/outboxer"
)

func main() {
	outboxer.Run(context.Background(), os.Args[1:])
}
