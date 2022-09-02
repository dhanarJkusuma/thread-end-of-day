package main

import (
	"encoding/csv"
	"log"
	"os"
)

type CSVReader struct {
}

func NewCSVReader() *CSVReader {
	return &CSVReader{}
}

func (csr *CSVReader) Parse(filepath string) [][]string {
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	rows, err := csv.NewReader(f).ReadAll()
	f.Close()
	if err != nil {
		log.Fatal(err)
	}
	return rows
}
