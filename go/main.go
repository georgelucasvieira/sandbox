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
	CountTo   int
	Consider  bool
}

func fetchPaginatedResults(page, limit int) {
	numGoroutines := 10
	chunkSize := 1000 / numGoroutines //100
	offset := (page - 1) * limit
	fmt.Println("Offset:", offset, "Limit:", limit)

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
	var counter int
	var lastCounterSum int
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
				fmt.Println("Canal->", countChan.Index, "Encontrou", countChan.Count, "itens.", "TotalRows =", stagingSum)

				if stagingSum > offset {
					start = true
					counter += countChan.Count
					if !offsetEnded {
						if offset == 0 {
							countFrom = 0
						} else {
							countFrom = offset - totalRows + 1
						}
						offsetEnded = true
						fmt.Println("Canal->", countChan.Index, "Offset de", offset, "itens atingido, começar a contar a partir do item", countFrom)
					}
				}

				totalRows = stagingSum

				indexAndCount := IndexAndCount{
					Index:     countChan.Index,
					Count:     countChan.Count,
					Consider:  start,
					CountFrom: countFrom,
					CountTo:   counter,
				}

				if counter >= offset+limit {
					fmt.Println("Canal->", countChan.Index, "Iniciando effectiveSearch...")
					counter = offset + limit

					counter -= lastCounterSum
					indexAndCount.CountTo = counter
					indexesAndCountToSearch = append(indexesAndCountToSearch, indexAndCount)
					effectiveSearch(indexesAndCountToSearch)
				}
				lastCounterSum += indexAndCount.CountTo
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
		if !index.Consider {
			fmt.Println("Ignorando o canal", index.Index, "com", index.Count, "itens")
			close(resultChans[i])
			continue
		}

		if index.Consider {
			fmt.Println("Começando a buscar resultados a partir do item", index.CountFrom, "do canal", index.Index)
		}

		go func(i int, index IndexAndCount) {
			result := Result{
				Index: index.Index,
			}

			for x := 0; x < index.Count; x++ {
				if x >= index.CountFrom && x < index.CountTo {
					result.Results = append(result.Results, fmt.Sprint("Resultado: { item ", x, " do canal ", index.Index, " }"))
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
				fmt.Println(out, "| Total de itens ->", sum)
			}
		}
	}

}

//corrigir o countTo
