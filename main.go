package main

import (
	"encoding/csv"
	"fmt"
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

func main() {
	numbers, err := readNumbersFromCSV("numbers.csv")
	if err != nil {
		log.Fatalf("Error reading numbers: %v", err)
	}

	start := time.Now()

	sortedNumbers := RunMergeSort(numbers)

	elapsed := time.Since(start)
	//fmt.Printf("Original numbers: %v\n", numbers)
	//fmt.Printf("Sorted numbers  : %v\n", sortedNumbers)
	fmt.Printf("MergeSort took %s\n", elapsed)

	err = writeNumbersToCSV("out.csv", sortedNumbers)
	if err != nil {
		log.Fatalf("Error writing numbers: %v", err)
	}

	if isSorted(sortedNumbers) {
		fmt.Println("The numbers are sorted correctly")
	} else {
		fmt.Println("The numbers are not sorted correctly")
	}
}

func readNumbersFromCSV(filename string) ([]int, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

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

func writeNumbersToCSV(filename string, numbers []int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, number := range numbers {
		err := writer.Write([]string{strconv.Itoa(number)})
		if err != nil {
			return err
		}
	}

	return nil
}
func isSorted(numbers []int) bool {
	for i := 1; i < len(numbers); i++ {
		if numbers[i-1] > numbers[i] {
			return false
		}
	}
	return true

}
