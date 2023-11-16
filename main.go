package main

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func Merge(left, right []int) []int {
	merged := make([]int, 0, len(left)+len(right))

	for len(left) > 0 || len(right) > 0 {
		if len(left) == 0 {
			return append(merged, right...)
		} else if len(right) == 0 {
			return append(merged, left...)
		} else if left[0] < right[0] {
			merged = append(merged, left[0])
			left = left[1:]
		} else {
			merged = append(merged, right[0])
			right = right[1:]
		}
	}

	return merged
}

func SingleMergeSort(data []int) []int {
	if len(data) <= 1 {
		return data
	}

	mid := len(data) / 2
	left := SingleMergeSort(data[:mid])
	right := SingleMergeSort(data[mid:])

	return Merge(left, right)
}

func ConcurrentMergeSort(data []int, c chan struct{}) []int {
	if len(data) <= 1 {
		return data
	}

	mid := len(data) / 2

	var wg sync.WaitGroup
	wg.Add(2)

	var leftData []int
	var rightData []int

	select {
	case c <- struct{}{}:
		go func() {
			leftData = ConcurrentMergeSort(data[:mid], c)
			wg.Done()
		}()
	default:
		leftData = SingleMergeSort(data[:mid])
		wg.Done()
	}

	select {
	case c <- struct{}{}:
		go func() {
			rightData = ConcurrentMergeSort(data[mid:], c)
			wg.Done()
		}()
	default:
		rightData = SingleMergeSort(data[mid:])
		wg.Done()
	}

	wg.Wait()
	return Merge(leftData, rightData)
}

func RunMergeSort(data []int) []int {
	c := make(chan struct{}, 4)
	return ConcurrentMergeSort(data, c)
}

func timeTrack(start time.Time, name string, operation func() []int) []int {
	defer func() {
		elapsed := time.Since(start)
		log.Printf("%s took %s", name, elapsed)
	}()

	// Execute the specified operation and return the result.
	return operation()
}

func main() {
	// Read numbers from the input CSV file.
	numbers, err := readNumbersFromCSV("random_numbers.csv")
	if err != nil {
		log.Fatalf("Error reading numbers: %v", err)
	}

	start := time.Now()
	sortedNumbers := timeTrack(start, "Sorting", func() []int {
		return RunMergeSort(numbers)
	})

	// Print the original and sorted numbers.
	//fmt.Printf("Original numbers: %v\n", numbers)
	//fmt.Printf("Sorted numbers  : %v\n", sortedNumbers)

	// Write the sorted numbers to the output CSV file. Change the filename to yours.
	err = writeNumbersToCSV("out1m.csv", sortedNumbers)
	if err != nil {
		log.Fatalf("Error writing numbers: %v", err)
	}
}

// readNumbersFromCSV reads integers from a CSV file and returns them as a slice.
func readNumbersFromCSV(filename string) ([]int, error) {
	// Open the input CSV file.
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Create a new CSV reader.
	reader := csv.NewReader(file)

	// Read the numbers from the CSV file and store them in a slice.
	var numbers []int
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		for _, value := range record {
			number, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			numbers = append(numbers, number)
		}
	}

	return numbers, nil
}

// writeNumbersToCSV writes a slice of integers to a CSV file.
func writeNumbersToCSV(filename string, numbers []int) error {
	// Create the output file.
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a new CSV writer.
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the numbers to the CSV file.
	for _, number := range numbers {
		err := writer.Write([]string{strconv.Itoa(number)})
		if err != nil {
			return err
		}
	}

	return nil
}
