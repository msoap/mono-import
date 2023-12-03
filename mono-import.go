/*
Importing CSV data from monobank to SQLite DB
Usage:

	go run mono-import.go -db=mono.db mono_*.csv
*/
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

type record struct {
	CreatedAt  time.Time `db:"created_at"`
	Title      string    `db:"title"`
	MCC        int       `db:"mcc"`
	Amount     int       `db:"amount"`      // in UAH * 100 (kopecks)
	AmountOrig int       `db:"amount_orig"` // in original currency (USD/EUR): V * 100 (cents)
	Currency   string    `db:"currency"`    // UAH/USD/EUR
	Exchange   float64   `db:"exchange"`    // exchange rate
	Commission int       `db:"commission"`  // in UAH * 100
	Cashback   int       `db:"cashback"`    // in UAH * 100
	Rest       int       `db:"rest"`        // in UAH * 100
}

func main() {
	dbName := ""
	flag.StringVar(&dbName, "db", "mono.db", "SQLite DB name")
	flag.Parse()
	fmt.Printf("Importing to %s\n", dbName)

	allData := [][]string{}
	// iterate over all files
	for _, filename := range flag.Args() {
		fmt.Printf("Importing from %s\n", filename)

		// read CSV file
		data, err := readCSV(filename)
		if err != nil {
			log.Fatalf("Error reading CSV file: %s", err)
		}
		// remove header
		data = data[1:]
		allData = append(allData, data...)
	}

	for _, row := range allData {
		fmt.Printf("%#v\n", row)
	}
}

func readCSV(filename string) ([][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return csv.NewReader(f).ReadAll()
}
