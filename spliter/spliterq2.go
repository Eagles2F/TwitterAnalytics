package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	w1   *os.File
	w2   *os.File
	w3   *os.File
	w4   *os.File
	w5   *os.File
	w6   *os.File
	w7   *os.File
	w8   *os.File
	w9   *os.File
	w10  *os.File
	wmap map[uint32]*os.File
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func split(path string) {
	r, _ := os.Open(path)
	defer r.Close()

	scanner := bufio.NewScanner(r)

	var buffer bytes.Buffer
	// uid timestamp (float) tid text score
	for scanner.Scan() {
		l := scanner.Text()
		vars := strings.Split(l, "\t")
		loc := hash(vars[0])%10 + 1

		it, _ := strconv.ParseFloat(vars[1], 32)
		st := strconv.Itoa((int)(it))
		buffer.WriteString(vars[0] + "," + st + "\t")

		// tid, score, text
		buffer.WriteString(vars[2] + "," + vars[4] + "," + vars[3] + "\n")
		w := wmap[loc]
		w.WriteString(buffer.String())
		buffer.Reset()
	}
}

func main() {
	w1, _ := os.OpenFile("tweets_q2_1", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w2, _ := os.OpenFile("tweets_q2_2", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w3, _ := os.OpenFile("tweets_q2_3", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w4, _ := os.OpenFile("tweets_q2_4", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w5, _ := os.OpenFile("tweets_q2_5", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	w6, _ := os.OpenFile("tweets_q2_6", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w7, _ := os.OpenFile("tweets_q2_7", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w8, _ := os.OpenFile("tweets_q2_8", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w9, _ := os.OpenFile("tweets_q2_9", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w10, _ := os.OpenFile("tweets_q2_10", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	wmap = make(map[uint32]*os.File)
	wmap[1] = w1
	wmap[2] = w2
	wmap[3] = w3
	wmap[4] = w4
	wmap[5] = w5

	wmap[6] = w6
	wmap[7] = w7
	wmap[8] = w8
	wmap[9] = w9
	wmap[10] = w10

	defer w1.Close()
	defer w2.Close()
	defer w3.Close()
	defer w4.Close()
	defer w5.Close()
	defer w6.Close()
	defer w7.Close()
	defer w8.Close()
	defer w9.Close()
	defer w10.Close()

	filepath.Walk("./file2", func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			split(path)
			fmt.Println("finished ", path)
		}
		return nil
	})
}
