package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/joho/godotenv"
)

var (
	store = flag.String("store", "/tmp/tradingstore", "Path to on-disk datastore")
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}
	alpaca.SetBaseUrl("https://paper-api.alpaca.markets")
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func run() error {
	_, err := NewPersistantStore(*store)
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	client := alpaca.NewClient(common.Credentials())
	acct, err := client.GetAccount()
	if err != nil {
		return fmt.Errorf("failed to get alpaca account data: %w", err)
	}
	log.Printf("Account ID: %s", acct.ID)
	return nil
}
