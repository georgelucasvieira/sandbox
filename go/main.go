package main

import (
	"fmt"
)

type Record struct {
	ID   int
	Data string
}

func main() {
	fetchPaginatedResults(2, 20)
}

type IndexAndCount struct {
	Index int
	Count int
}

func fetchPaginatedResults(page, limit int) {
	numGoroutines := 10
	chunkSize := 1000 / numGoroutines //100
	offset := (page - 1) * limit

	type CountResult struct {
		Index int
		Count int
		Start int
		End   int
	}

	countChans := make([]chan CountResult, numGoroutines)
	for i := range countChans {
		countChans[i] = make(chan CountResult)
	}

	for i := 0; i < numGoroutines; i++ {
		go func(index, start, end int) {
			count := end / (9 + index)
			countChans[index] <- CountResult{Index: index, Count: int(count), Start: start, End: end}
		}(i, i*chunkSize+1, (i+1)*chunkSize)
	}

	var indexesAndCountToSearch []IndexAndCount

	for i := range countChans {
		var sum int
		for countChan := range countChans[i] {
			if sum >= offset+limit {
				close(countChans[i])
				continue
			}

			if countChan.Count > 0 {
				sum += countChan.Count
				indexAndCount := IndexAndCount{
					Index: countChan.Index,
					Count: countChan.Count,
				}

				if indexAndCount.Count >= offset+limit {
					if indexAndCount.Count > offset+limit {
						indexAndCount.Count = indexAndCount.Count - (offset + limit)
					}
					indexesAndCountToSearch = append(indexesAndCountToSearch, indexAndCount)
					effectiveSearch(page, limit, offset, indexesAndCountToSearch)
				}
			}

			close(countChans[i])
		}
	}
}

func effectiveSearch(page, limit, offset int, indexes []IndexAndCount) {
	numGoroutines := len(indexes)

	type Result struct {
		Index  int
		Result []string
	}

	resultChans := make([]chan Result, numGoroutines)
	for i := range resultChans {
		resultChans[i] = make(chan Result)
	}

	for i, index := range indexes {
		go func(i int, index IndexAndCount) {
			result := Result{
				Index: index.Index,
			}

			for x := 0; x < index.Count; x++ {
				result.Result = append(result.Result, fmt.Sprint("resultado", x, "do index", index.Index))
			}

			resultChans[i] <- result
		}(i, index)
	}

	//TODO: consumir esses channels

}
