package main

import (
	"fmt"
	"log"
	"sandbox/database"

	"github.com/joho/godotenv"
)

type Record struct {
	ID   int
	Data string
}

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	err = database.NewConnection()
	if err != nil {
		log.Panic(err)
	}

	err = fetchPaginatedResults(2, 7)
	if err != nil {
		log.Panic(err)
	}
}

type Search struct {
	Index     int
	Count     int
	CountFrom int
	CountTo   int
	Consider  bool
}

func fetchPaginatedResults(page, limit int) error {
	numGoroutines := 10
	chunkSize := 1000 / numGoroutines //100
	offset := (page - 1) * limit
	fmt.Println("Offset:", offset, "Limit:", limit)

	fmt.Println("Iniciando busca por", limit, "itens", "na pagina", page)

	searchChans := make([]chan Search, numGoroutines)
	for i := range searchChans {
		searchChans[i] = make(chan Search)
	}

	for i := 0; i < numGoroutines; i++ {
		go func(index, start, end int) {
			count := end / (9 + index)
			searchChans[index] <- Search{Index: index, Count: int(count), CountFrom: start, CountTo: end}
		}(i, i*chunkSize+1, (i+1)*chunkSize)
	}

	var searchResults []Search

	var totalRows int
	var offsetAchieved bool
	var consider bool

	var rightLimit int

	var initCountFrom int

	for i := range searchChans {
		for countChan := range searchChans[i] {
			if totalRows >= offset+limit {
				totalRows += countChan.Count
				fmt.Println("Canal->", countChan.Index, "Encontrou", countChan.Count, "itens.", "TotalRows =", totalRows)
				close(searchChans[i])
				continue
			}

			if countChan.Count == 0 {
				close(searchChans[i])
				continue
			}

			stagingSum := totalRows + countChan.Count
			fmt.Println("Canal->", countChan.Index, "Encontrou", countChan.Count, "itens.", "TotalRows =", stagingSum)

			countFrom := 0

			//left limit
			if stagingSum > offset {
				consider = true

				if !offsetAchieved {
					if offset == 0 {
						countFrom = 0
					} else {
						countFrom = offset - totalRows + 1
					}
					initCountFrom = countFrom
					offsetAchieved = true
					fmt.Println("Canal->", countChan.Index, "Offset de", offset, "itens atingido, começar a contar a partir do item", initCountFrom, "do Canal ->", countChan.Index)

				}

			}

			totalRows = stagingSum

			searchResult := Search{
				Index:     countChan.Index,
				Count:     countChan.Count,
				Consider:  consider,
				CountFrom: countFrom,
				CountTo:   countChan.Count,
			}

			//right limit
			if totalRows >= offset+limit {
				fmt.Println("Canal->", countChan.Index, "Iniciando effectiveSearch...")
				rightLimit = totalRows - (offset + limit)
				countTo := searchResult.Count - rightLimit

				searchResult.CountTo = countTo
				searchResults = append(searchResults, searchResult)

				fmt.Println("Canal->", countChan.Index, "Limit de", limit, "itens atingido, contar até o item", countTo, "do Canal ->", countChan.Index)

				effectiveSearch(searchResults)
			}

			searchResults = append(searchResults, searchResult)
			close(searchChans[i])
		}
	}
	fmt.Println("finalizado!")
	return nil
}

func effectiveSearch(indexes []Search) {
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

		go func(i int, index Search) {
			result := Result{
				Index: index.Index,
			}

			for x := 0; x < index.Count; x++ {
				if x >= index.CountFrom && x <= index.CountTo {
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
