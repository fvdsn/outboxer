package main

import (
	"context"

	"outboxer-go/internal/outboxer"
)

func main() {
	outboxer.Run(context.Background())
}
