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
	Index     int
	Count     int
	CountFrom int
	Consider  bool
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
	var offsetEnded bool

	for i := range countChans {
		for countChan := range countChans[i] {
			if totalRows >= offset+limit {
				totalRows += countChan.Count
				fmt.Println("Total Rows:", totalRows, ", Canal", countChan.Index)
				close(countChans[i])
				continue
			}

			if countChan.Count > 0 {
				consider := false
				countFrom := 0

				stagingSum := totalRows + countChan.Count

				if stagingSum > offset {
					consider = true
					if !offsetEnded {
						countFrom = offset - totalRows
						offsetEnded = true
					}
				}

				totalRows = stagingSum

				fmt.Println("Total Rows:", totalRows, ", Canal", countChan.Index)

				indexAndCount := IndexAndCount{
					Index:     countChan.Index,
					Count:     countChan.Count,
					Consider:  consider,
					CountFrom: countFrom,
				}
				if totalRows >= offset+limit {
					fmt.Println("Temos itens o suficiente até o canal", indexAndCount.Index, "iniciando effectiveSearch")
					if totalRows > offset+limit {
						indexAndCount.Count = totalRows - (offset + limit)
					}
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
	fmt.Print("Iniciando effective search...")
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
		if !index.Consider {
			fmt.Println("Pulando", index.Count, "valores de", index.Count, "no canal", index.Index)
			continue
		}

		go func(i int, index IndexAndCount) {
			result := Result{
				Index: index.Index,
			}

			if index.CountFrom != 0 {
				fmt.Println("Thread: Pulando", index.CountFrom, "valores de", index.Count, "no canal", index.Index)
			}

			for x := 0; x < index.Count; x++ {
				if x >= index.CountFrom {
					result.Results = append(result.Results, fmt.Sprint("resultado", x, "do index", index.Index))
				}
			}

			resultChans[i] <- result
		}(i, index)
	}

	for i := range resultChans {
		if len(resultChans[i]) == 0 {
			close(resultChans[i])
		}

		for result := range resultChans[i] {
			println(result.Results)
			close(resultChans[i])
		}
	}

}
