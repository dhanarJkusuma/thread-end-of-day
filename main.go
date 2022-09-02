package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"alami/workerpool"
)

type CalculationResult struct {
	Average  int
	ThreadNo int
}

func main() {
	beforeEODFile := "./Before Eod.csv"
	afterEODFile := "./After Eod.csv"

	// Start Worker Pool.
	totalWorker := 7
	wg := new(sync.WaitGroup)

	csvReader, csvFile, err := openCsvFile(beforeEODFile)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	//read
	resultsMap := make(map[string]CalculationResult)
	data := readAllCSV(csvReader, resultsMap)

	//workerpool
	wp := workerpool.NewWorkerPool(totalWorker, wg)
	wp.Run()

	putAverageData(data, wp)
	putBenefitFreeTransfer(data, wp)
	putBenefitBalance(data, wp)

	wp = workerpool.NewWorkerPool(8, wg)
	wp.Run()
	putBonusBalance(data, wp)

	wg.Wait()
	writeCSV(afterEODFile, data)
}

func putAverageData(data [][]interface{}, wp workerpool.WorkerPool) [][]interface{} {
	length := len(data)
	for i := 0; i < length; i++ {
		rowOrdered := data[i]
		idx := i
		wp.AddTask(func(workerID int) {
			//idColumn := 0
			balancedColumn := 3
			previousBalanceColumn := 4
			avgBalanceColumn := 5
			balance := rowOrdered[balancedColumn]

			previousBalance := rowOrdered[previousBalanceColumn]

			balanceNumber, _ := strconv.Atoi(balance.(string))
			previousBalanceNumber, _ := strconv.Atoi(previousBalance.(string))

			avgBalance := (balanceNumber + previousBalanceNumber) / 2

			//append worker id
			rowOrdered[avgBalanceColumn] = fmt.Sprintf("%d", avgBalance)
			//column: 7 worker ID average
			rowOrdered = append(rowOrdered, fmt.Sprintf("%d", workerID))

			data[idx] = rowOrdered
		})
	}
	return data
}

func putBenefitFreeTransfer(data [][]interface{}, wp workerpool.WorkerPool) [][]interface{} {
	length := len(data)
	for i := 0; i < length; i++ {
		rowOrdered := data[i]
		idx := i
		wp.AddTask(func(workerID int) {
			//idColumn := 0
			balancedColumn := 3
			freeTransferColumn := 6
			balance := rowOrdered[balancedColumn]
			freeTransfer := rowOrdered[freeTransferColumn]
			balanceNumber, _ := strconv.Atoi(balance.(string))
			freeTransferNumber, _ := strconv.Atoi(freeTransfer.(string))
			if balanceNumber >= 100 && balanceNumber <= 150 {
				freeTransferNumber += 5
			}

			//append worker id
			rowOrdered[freeTransferColumn] = fmt.Sprintf("%d", freeTransferNumber)

			//column 8: free transfer worker ID
			rowOrdered = append(rowOrdered, fmt.Sprintf("%d", workerID))

			data[idx] = rowOrdered
		})
	}
	return data
}

func putBenefitBalance(data [][]interface{}, wp workerpool.WorkerPool) [][]interface{} {
	length := len(data)
	for i := 0; i < length; i++ {
		rowOrdered := data[i]
		idx := i
		wp.AddTask(func(workerID int) {
			//idColumn := 0
			balancedColumn := 3
			balance := rowOrdered[balancedColumn]
			balanceNumber, _ := strconv.Atoi(balance.(string))
			if balanceNumber > 150 {
				balanceNumber += 25
			}

			//append worker id
			rowOrdered[balancedColumn] = fmt.Sprintf("%d", balanceNumber)
			//column: 9, benefit balance worker id
			rowOrdered = append(rowOrdered, fmt.Sprintf("%d", workerID))

			data[idx] = rowOrdered
		})
	}
	return data
}

func putBonusBalance(data [][]interface{}, wp workerpool.WorkerPool) [][]interface{} {
	length := len(data)
	for i := 0; i < length; i++ {
		rowOrdered := data[i]
		idx := i
		if idx+1 <= 100 {
			wp.AddTask(func(workerID int) {
				idColumn := 0
				balancedColumn := 3
				balance := rowOrdered[balancedColumn]
				id := rowOrdered[idColumn]
				balanceNumber, _ := strconv.Atoi(balance.(string))
				idNumber, _ := strconv.Atoi(id.(string))

				if idNumber <= 100 {
					balanceNumber += 10
				}

				//append worker id
				rowOrdered[balancedColumn] = fmt.Sprintf("%d", balanceNumber)
				//column: 10, bonus 100 first balance worker ID
				rowOrdered = append(rowOrdered, fmt.Sprintf("%d", workerID))

				data[idx] = rowOrdered
			})
		}
	}
	return data
}

func openCsvFile(csvFile string) (*csv.Reader, *os.File, error) {
	log.Println("=> open csv file")

	f, err := os.Open(csvFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Fatal("file csvFile tidak ditemukan.")
		}

		return nil, nil, err
	}

	reader := csv.NewReader(f)
	reader.Comma = ';'
	return reader, f, nil
}

func writeCSV(filepath string, data [][]interface{}) {
	csvFile, err := os.Create(filepath)
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	csvwriter := csv.NewWriter(csvFile)
	header := make([]string, 11)
	header[0] = "id"
	header[1] = "Nama"
	header[2] = "Age"
	header[3] = "Balanced"
	header[4] = "No 2b Thread-No"
	header[5] = "No 3 Thread-No"
	header[6] = "Previous Balanced"
	header[7] = "Average Balanced"
	header[8] = "No 1 Thread-No"
	header[9] = "Free Transfer"
	header[10] = "No 2a Thread-No"

	_ = csvwriter.Write(header)
	for _, empRow := range data {
		dataResult := make([]string, 11)
		dataResult[0] = empRow[0].(string)
		dataResult[1] = empRow[1].(string)
		dataResult[2] = empRow[2].(string)
		dataResult[3] = empRow[3].(string)
		dataResult[4] = empRow[9].(string)
		if len(empRow) == 11 {
			dataResult[5] = empRow[10].(string)
		} else {
			dataResult[5] = ""
		}

		dataResult[6] = empRow[4].(string)
		dataResult[7] = empRow[5].(string)
		dataResult[8] = empRow[7].(string)
		dataResult[9] = empRow[6].(string)
		dataResult[10] = empRow[8].(string)
		_ = csvwriter.Write(dataResult)
	}
	csvwriter.Flush()
	csvFile.Close()
}

func readAllCSV(csvReader *csv.Reader, resultMap map[string]CalculationResult) [][]interface{} {
	results := make([][]interface{}, 0)
	isHeader := true
	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}

		if isHeader {
			isHeader = false
			continue
		}

		rowOrdered := make([]interface{}, 0)
		for _, each := range row {
			rowOrdered = append(rowOrdered, each)
		}
		results = append(results, rowOrdered)
	}
	return results
}
