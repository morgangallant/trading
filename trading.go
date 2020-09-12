package main

import (
	"fmt"
	"log"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}
	alpaca.SetBaseUrl("https://paper-api.alpaca.markets")
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func run() error {
	client := alpaca.NewClient(common.Credentials())
	acct, err := client.GetAccount()
	if err != nil {
		return fmt.Errorf("failed to get alpaca account data: %w", err)
	}
	log.Printf("Account ID: %s", acct.ID)
	return nil
}
