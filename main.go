package main

import (
	"bytes"
	"github.com/dolthub/swiss"
	"io"
	"os"
	"strconv"
	"sync"
)

const (
	filePath   = "/Users/admin/projects/rust/1brc-rust/measurements.txt"
	bufferSize = 2048 * 2048
	workersNum = 25
)

type Temperatures struct {
	Station string
	Cnt     int
	Min     float64
	Mean    float64
	Max     float64
}

func NewTemperatures(min, mean, max float64, cnt int, station string) *Temperatures {
	return &Temperatures{Min: min, Mean: mean, Max: max, Cnt: cnt, Station: station}
}

type incompleteLine struct {
	p       []byte // data
	n       int    // chunk number
	isFirst bool   // is first line? if not, then it's mean that it's the last
}

func main() {
	//pprofFile, err := os.Create("cpu.pprof")
	//if err != nil {
	//	panic(err)
	//}
	//defer func() { _ = pprofFile.Close() }()
	//
	////pprofFile, _ := os.Open(os.DevNull)
	////defer func() { _ = pprofFile.Close() }()
	//_ = pprof.StartCPUProfile(pprofFile)
	//defer pprof.StopCPUProfile()

	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	mu := new(sync.Mutex)
	wg := &sync.WaitGroup{}

	resultMap := swiss.NewMap[uint64, *Temperatures](500)

	incompleteLinesCh := make(chan incompleteLine, workersNum-2)
	go processIncompleteLine(incompleteLinesCh, resultMap)

	resCh := make(chan *swiss.Map[uint64, *Temperatures], workersNum)
	wg.Add(1)
	go processChunks(wg, mu, resCh, incompleteLinesCh)

	wg.Add(1)
	go aggregateResults(wg, resCh, resultMap)

	wg.Wait()

	printResult(resultMap)
}

func processChunks(
	mwg *sync.WaitGroup,
	mu *sync.Mutex,
	resMapsCh chan<- *swiss.Map[uint64, *Temperatures],
	incompleteLinesCh chan<- incompleteLine,
) {
	defer func() {
		close(resMapsCh)
		close(incompleteLinesCh)
		mwg.Done()
	}()

	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for chunk := 0; chunk < workersNum; chunk++ {
		wg.Add(1)
		go func(chunk int) {
			defer wg.Done()

			resMap := swiss.NewMap[uint64, *Temperatures](100)

			buffer := make([]byte, bufferSize)
			for {
				mu.Lock()
				_, err = f.Read(buffer)
				mu.Unlock()
				if err != nil {
					if err == io.EOF {
						break
					} else {
						panic(err)
					}
				}

				// skip the last incomplete line and pass it to appropriate handler
				lastCorrectIdx := len(buffer)
				if chunk != 0 {
					lastCorrectIdx = bytes.LastIndexByte(buffer, '\n') + 1
					incompleteLinesCh <- incompleteLine{
						p:       buffer[lastCorrectIdx:],
						n:       chunk,
						isFirst: false,
					}
				}

				firstCorrectIdx := 0
				if chunk != workersNum-1 {
					firstCorrectIdx = bytes.IndexByte(buffer, '\n')
					incompleteLinesCh <- incompleteLine{
						p:       buffer[:firstCorrectIdx],
						n:       chunk,
						isFirst: true,
					}
				}

				buffer = buffer[firstCorrectIdx:lastCorrectIdx]

				s := 0
				for i := 0; i < len(buffer); i++ {
					if buffer[i] == '\n' {
						var station []byte
						temperature := 0.00
						for j := 0; j < len(buffer[s:i]); j++ {
							if buffer[s:i][j] == ';' {
								station = buffer[s:i][:j]
								temperature = parseFloat(buffer[s:i][j+1:])
							}
						}

						h := hash(station)

						temps, found := resMap.Get(h)
						if !found {
							resMap.Put(h, NewTemperatures(temperature, temperature, temperature, 1, string(station)))
						} else {
							temps.Cnt++
							temps.Mean += temperature

							if temperature < temps.Min {
								temps.Min = temperature
							} else if temperature > temps.Max {
								temps.Max = temperature
							}
						}

						s = i + 1
					}
				}
			}

			resMapsCh <- resMap
		}(chunk)
	}
}

func processIncompleteLine(incompleteLinesCh <-chan incompleteLine, resultsMap *swiss.Map[uint64, *Temperatures]) {
	for _ = range incompleteLinesCh {
		//fmt.Printf("data: %v, n: %d, isFirst: %v \n", string(line.p), line.n, line.isFirst)
	}
}

func aggregateResults(wg *sync.WaitGroup, resultsCh <-chan *swiss.Map[uint64, *Temperatures], resultsMap *swiss.Map[uint64, *Temperatures]) {
	defer wg.Done()

	for resMap := range resultsCh {
		resMap.Iter(func(h uint64, stat *Temperatures) (stop bool) {
			temps, found := resultsMap.Get(h)
			if !found {
				resultsMap.Put(h, NewTemperatures(stat.Min, stat.Mean, stat.Max, 1, stat.Station))
			} else {
				temps.Cnt++
				temps.Mean += stat.Mean

				if stat.Min < temps.Min {
					temps.Min = stat.Min
				} else if stat.Max > temps.Max {
					temps.Max = stat.Max
				}
			}
			return false
		})
	}
}

func printResult(resultsMap *swiss.Map[uint64, *Temperatures]) {
	i := 0
	l := resultsMap.Count()
	_, _ = os.Stdout.WriteString("{")
	resultsMap.Iter(func(_ uint64, stat *Temperatures) (stop bool) {
		_, _ = os.Stdout.WriteString(
			stat.Station + "=" + strconv.FormatFloat(stat.Min, 'f', 2, 64) +
				"/" + strconv.FormatFloat(stat.Mean/float64(stat.Cnt), 'f', 2, 64) + "/" +
				strconv.FormatFloat(stat.Max, 'f', 2, 64),
		)
		if i < l-1 {
			_, _ = os.Stdout.WriteString(", ")
		}
		i++
		return false
	})
	_, _ = os.Stdout.WriteString("}\n")
}

func parseFloat(s []byte) float64 {
	negative := false
	index := 0
	if s[index] == '-' {
		index++
		negative = true
	}
	temp := float64(s[index] - '0')
	index++
	if s[index] != '.' {
		temp = temp*10 + float64(s[index]-'0')
		index++
	}
	index++
	temp += float64(s[index]-'0') / 10
	if negative {
		temp = -temp
	}
	return temp
}

func hash(str []byte) uint64 {
	var h uint64 = 5381
	for _, b := range str {
		h = (h << 5) + h + uint64(b)
	}
	return h
}
