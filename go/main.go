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
	Index      int
	Count      int
	CountFrom  int
	CountTo    int
	StartCount bool
}

func fetchPaginatedResults(page, limit int) {
	numGoroutines := 10
	chunkSize := 1000 / numGoroutines //100
	offset := (page - 1) * limit

	fmt.Println("Iniciando busca por", limit, "itens", "na pagina", page)

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

	var totalRows int
	var countTo int
	var offsetEnded bool

	for i := range countChans {
		for countChan := range countChans[i] {
			if totalRows >= offset+limit {
				totalRows += countChan.Count
				fmt.Println("Canal->", countChan.Index, "Encontrou", countChan.Count, "itens.", "TotalRows =", totalRows)
				close(countChans[i])
				continue
			}

			if countChan.Count > 0 {
				start := false
				countFrom := 0

				stagingSum := totalRows + countChan.Count

				if stagingSum > offset {
					start = true
					countTo += countChan.Count
					if !offsetEnded {
						countFrom = offset - totalRows
						offsetEnded = true
						fmt.Println("Canal->", countChan.Index, "Offset", offset, "atingido, contar a partir do item", countFrom)
					}
				}

				totalRows = stagingSum

				fmt.Println("Canal->", countChan.Index, "Encontrou", countChan.Count, "itens.", "TotalRows =", totalRows)

				indexAndCount := IndexAndCount{
					Index:      countChan.Index,
					Count:      countChan.Count,
					StartCount: start,
					CountFrom:  countFrom,
					CountTo:    countTo,
				}

				if countTo >= offset+limit {
					fmt.Println("Canal->", countChan.Index, "Iniciando effectiveSearch...")
					countTo = countTo - (offset + limit)
					indexAndCount.CountTo = countTo
					indexesAndCountToSearch = append(indexesAndCountToSearch, indexAndCount)
					effectiveSearch(indexesAndCountToSearch)
				}
				indexesAndCountToSearch = append(indexesAndCountToSearch, indexAndCount)
			}

			close(countChans[i])
		}
	}
	fmt.Println("finalizado!")
}

func effectiveSearch(indexes []IndexAndCount) {
	numGoroutines := len(indexes)

	type Result struct {
		Index   int
		Results []string
	}

	resultChans := make([]chan Result, numGoroutines)
	for i := range resultChans {
		resultChans[i] = make(chan Result)
	}

	for i, index := range indexes {
		if !index.StartCount {
			fmt.Println("Pulando", index.Count, "valores de", index.Count, "no canal", index.Index)
			close(resultChans[i])
			continue
		}

		go func(i int, index IndexAndCount) {
			result := Result{
				Index: index.Index,
			}

			for x := 0; x < index.Count; x++ {
				if x >= index.CountFrom && x < index.CountTo {
					result.Results = append(result.Results, fmt.Sprint("resultado do item ", x, " do index ", index.Index))
				}
			}

			resultChans[i] <- result
			close(resultChans[i])
		}(i, index)
	}

	var sum int
	for _, resultChan := range resultChans {
		for result := range resultChan {
			for _, out := range result.Results {
				sum += 1
				fmt.Println(out, "audit total de itens:", sum)
			}
		}
	}

}

//corrigir o countTo
