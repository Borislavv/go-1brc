package main

import (
	"bytes"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"syscall"
)

const path = "/Users/admin/projects/rust/1brc-rust/measurements.txt"

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

type chunk struct {
	data []byte
}

func main() {
	fff, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	defer func() { _ = fff.Close() }()

	// Запускаем профилирование CPU
	_ = pprof.StartCPUProfile(fff)
	defer pprof.StopCPUProfile()

	ff, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer func() { _ = ff.Close() }()

	st, err := ff.Stat()
	if err != nil {
		panic(err)
	}

	bufferedFile, err := syscall.Mmap(int(ff.Fd()), 0, int(st.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		panic(err)
	}
	defer func() { _ = syscall.Munmap(bufferedFile) }()

	cpu := runtime.NumCPU()

	chunksCh := make(chan *chunk, cpu)
	go getChunks(chunksCh, bufferedFile, st.Size(), cpu)

	wg := &sync.WaitGroup{}

	resCh := make(chan map[uint64]*Temperatures, cpu)
	wg.Add(1)
	go processChunks(wg, chunksCh, resCh)

	resultsMap := make(map[uint64]*Temperatures, 500)
	wg.Add(1)
	go aggregateResults(wg, resCh, resultsMap)

	wg.Wait()

	printResult(resultsMap)
}

func getChunks(chunksCh chan<- *chunk, bufferedFile []byte, filesize int64, cpu int) {
	defer close(chunksCh)

	chunksize := filesize / int64(cpu)

	offset := int64(0)
	for i := 0; i < cpu; i++ {
		if i == cpu-1 {
			if offset < filesize {
				chunksCh <- &chunk{data: bufferedFile[offset:]}
			}
			break
		}

		seekOffset := offset + chunksize
		if seekOffset > filesize {
			seekOffset = filesize
		}

		lastNewlineIndex := bytes.LastIndexByte(bufferedFile[offset:seekOffset], '\n')
		nextOffset := offset + int64(lastNewlineIndex) + 1

		chunksCh <- &chunk{data: bufferedFile[offset:nextOffset]}

		offset = nextOffset
	}
}

func processChunks(wg *sync.WaitGroup, chunksCh <-chan *chunk, resCh chan<- map[uint64]*Temperatures) {
	defer func() {
		wg.Done()
		close(resCh)
	}()

	wgg := &sync.WaitGroup{}
	defer wgg.Wait()
	for chunkBuffer := range chunksCh {
		wgg.Add(1)
		go func(chunkData *chunk) {
			defer wgg.Done()

			resMap := make(map[uint64]*Temperatures, 100)

			s := 0
			l := len(chunkData.data)
			for i := 0; i < l; i++ {
				if chunkData.data[i] == '\n' {
					var station []byte
					temperature := 0.00
					for j := 0; j < len(chunkData.data[s:i]); j++ {
						if chunkData.data[s:i][j] == ';' {
							station = chunkData.data[s:i][:j]
							temperature = parseFloat(chunkData.data[s:i][j+1:])
						}
					}

					h := hash(station)

					temps, found := resMap[h]
					if !found {
						resMap[h] = NewTemperatures(temperature, temperature, temperature, 1, string(station))
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

			resCh <- resMap
		}(chunkBuffer)
	}
}

func aggregateResults(wg *sync.WaitGroup, resultsCh <-chan map[uint64]*Temperatures, resultsMap map[uint64]*Temperatures) {
	defer wg.Done()

	for resMap := range resultsCh {
		for h, stat := range resMap {

			temps, found := resultsMap[h]
			if !found {
				resultsMap[h] = NewTemperatures(stat.Min, stat.Mean, stat.Max, 1, stat.Station)
			} else {
				temps.Cnt++
				temps.Mean += stat.Mean

				if stat.Min < temps.Min {
					temps.Min = stat.Min
				} else if stat.Max > temps.Max {
					temps.Max = stat.Max
				}
			}
		}
	}
}

func printResult(resultsMap map[uint64]*Temperatures) {
	i := 0
	l := len(resultsMap)
	_, _ = os.Stdout.WriteString("{")
	for _, stat := range resultsMap {
		_, _ = os.Stdout.WriteString(
			stat.Station + "=" + strconv.FormatFloat(stat.Min, 'f', 2, 64) +
				"/" + strconv.FormatFloat(stat.Mean/float64(stat.Cnt), 'f', 2, 64) + "/" +
				strconv.FormatFloat(stat.Max, 'f', 2, 64),
		)
		if i < l-1 {
			_, _ = os.Stdout.WriteString(", ")
		}
		i++
	}
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
