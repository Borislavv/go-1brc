package main

import (
	"fmt"
	"github.com/dolthub/swiss"
	"os"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
)

const (
	filePath   = "/Users/admin/projects/rust/1brc-rust/measurements.txt"
	bufferSize = 2048 * 2048
	workersNum = 24
)

var lockIdx = 0
var offset atomic.Int64

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

type IncompleteLine struct {
	Idx     int
	Value   []byte
	Initial bool
}

func main() {
	pprofFile, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	defer func() { _ = pprofFile.Close() }()

	//pprofFile, _ := os.Open(os.DevNull)
	//defer func() { _ = pprofFile.Close() }()
	_ = pprof.StartCPUProfile(pprofFile)
	defer pprof.StopCPUProfile()

	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer func() { _ = f.Close() }()

	mu := new(sync.Mutex)

	resCh := make(chan *swiss.Map[uint64, *Temperatures], workersNum)
	resultMap := swiss.NewMap[uint64, *Temperatures](500)

	wg := &sync.WaitGroup{}

	incompleteLinesCh := make(chan *IncompleteLine, 1024)
	wg.Add(1)
	go trashBin(incompleteLinesCh, resCh, wg)

	wg.Add(1)
	go processChunks(wg, mu, resCh, incompleteLinesCh)

	wg.Add(1)
	go aggregateResults(wg, resCh, resultMap)

	wg.Wait()

	//printResult(resultMap)
}

func processChunks(
	mwg *sync.WaitGroup,
	mu *sync.Mutex,
	resMapsCh chan<- *swiss.Map[uint64, *Temperatures],
	incompleteLinesCh chan<- *IncompleteLine,
) {
	defer func() {
		close(incompleteLinesCh)
		mwg.Done()
	}()

	fd, err := syscall.Open(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer syscall.Close(fd)

	//var stat syscall.Stat_t
	//err = syscall.Fstat(fd, &stat)
	//if err != nil {
	//	panic(err)
	//}

	//chunksize := stat.Size / workersNum

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for worker := 0; worker < workersNum; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			fdd, derr := syscall.Open(filePath, syscall.O_RDONLY, 0)
			if derr != nil {
				fmt.Printf("Error opening file: %v\n", derr)
				os.Exit(1)
			}
			defer syscall.Close(fdd)

			resMap := swiss.NewMap[uint64, *Temperatures](100)
			
			buffer := make([]byte, bufferSize)
			for {
				//mu.Lock()
				//lockIdx++
				//idx := lockIdx
				//mu.Unlock()

				var off int64
				for {
					off = offset.Load()
					if offset.CompareAndSwap(off, off+bufferSize) {
						break
					}
				}
				n, rerr := syscall.Pread(fdd, buffer, off)
				if rerr != nil {
					panic(rerr)
				}
				if n == 0 {
					break
				}
				if n < bufferSize {
					buffer = buffer[:n]
				}

				//firstIdx := bytes.IndexByte(buffer, '\n')
				//incompleteLinesCh <- &IncompleteLine{
				//	Idx:     int(idx) - 1,
				//	Value:   buffer[:firstIdx],
				//	Initial: false,
				//}
				//lastIdx := bytes.LastIndexByte(buffer, '\n')
				//incompleteLinesCh <- &IncompleteLine{
				//	Idx:     int(idx),
				//	Value:   buffer[lastIdx+1:],
				//	Initial: true,
				//}

				//buffer = buffer[firstIdx+1 : lastIdx]

				statTo := 0
				stationTo := 0
				lineFrom := 0
				chunkLen := len(buffer)
				for lineTo := 0; lineTo < chunkLen; lineTo++ {
					if buffer[lineTo] == ';' {
						stationTo = lineTo
						statTo = lineTo + 1
					} else if buffer[lineTo] == '\n' {
						station := buffer[lineFrom:stationTo]
						temperature := parseFloat(buffer[statTo:])
						lineFrom = lineTo + 1

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
					}
				}
			}

			resMapsCh <- resMap
		}(worker)
	}
}

func trashBin(input chan *IncompleteLine, output chan *swiss.Map[uint64, *Temperatures], wg *sync.WaitGroup) {
	defer func() {
		close(output)
		wg.Done()
	}()
	data := swiss.NewMap[uint64, *Temperatures](1024)

	var can []*IncompleteLine
	buffer := make([]byte, 1024)

	for item := range input {
		can = append(can, item)
		can = saveCan(can, data, buffer)
	}

	output <- data
}

func saveCan(can []*IncompleteLine, data *swiss.Map[uint64, *Temperatures], buffer []byte) []*IncompleteLine {
	for i, ref := range can {
		if ref.Idx == 0 {
			_, nameInit, nameEnd, tempInit, tempEnd := nextLine(0, ref.Value)
			processLine(ref.Value[nameInit:nameEnd], ref.Value[tempInit:tempEnd], data)
			return slices.Delete(can, i, i+1)
		}

		for j, oth := range can {
			if ref.Idx == oth.Idx && i != j {
				if ref.Initial {
					copy(buffer[:len(ref.Value)], ref.Value)
					copy(buffer[len(ref.Value):], oth.Value)
				} else {
					copy(buffer[:len(oth.Value)], oth.Value)
					copy(buffer[len(oth.Value):], ref.Value)
				}
				total := len(ref.Value) + len(oth.Value)

				end, nameInit, nameEnd, tempInit, tempEnd := nextLine(0, buffer)
				processLine(buffer[nameInit:nameEnd], buffer[tempInit:tempEnd], data)

				if end < total {
					_, nameInit, nameEnd, tempInit, tempEnd := nextLine(end, buffer)
					processLine(buffer[nameInit:nameEnd], buffer[tempInit:tempEnd], data)
				}

				if i > j {
					can = slices.Delete(can, i, i+1)
					can = slices.Delete(can, j, j+1)
				} else {
					can = slices.Delete(can, j, j+1)
					can = slices.Delete(can, i, i+1)
				}

				return can
			}
		}
	}

	return can
}

func nextLine(readingIndex int, reading []byte) (nexReadingIndex, nameInit, nameEnd, tempInit, tempEnd int) {
	i := readingIndex
	nameInit = readingIndex
	for reading[i] != 59 { // ;
		i++
	}
	nameEnd = i

	i++ // skip ;

	tempInit = i
	for i < len(reading) && reading[i] != 10 { // \n
		i++
	}
	tempEnd = i

	readingIndex = i + 1
	return readingIndex, nameInit, nameEnd, tempInit, tempEnd
}

func processLine(name, temperature []byte, data *swiss.Map[uint64, *Temperatures]) {
	temp := parseFloat(temperature)
	id := hash(name)
	station, ok := data.Get(id)
	if !ok {
		data.Put(id, &Temperatures{string(name), 1, temp, temp, 1})
	} else {
		if temp < station.Min {
			station.Min = temp
		}
		if temp > station.Max {
			station.Max = temp
		}
		station.Mean += temp
		station.Cnt++
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
				temps.Cnt += stat.Cnt
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
