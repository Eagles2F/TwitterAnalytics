package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
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
	w11  *os.File
	w12  *os.File
	w13  *os.File
	w14  *os.File
	w15  *os.File
	w16  *os.File
	w17  *os.File
	w18  *os.File
	w19  *os.File
	w20  *os.File
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
		loc := hash(vars[0])%20 + 1

		// tid, text
		buffer.WriteString(l + "\n")
		w := wmap[loc]
		w.WriteString(buffer.String())
		buffer.Reset()
	}
}

func main() {
	w1, _ := os.OpenFile("tweets_q6_1", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w2, _ := os.OpenFile("tweets_q6_2", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w3, _ := os.OpenFile("tweets_q6_3", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w4, _ := os.OpenFile("tweets_q6_4", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w5, _ := os.OpenFile("tweets_q6_5", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	w6, _ := os.OpenFile("tweets_q6_6", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w7, _ := os.OpenFile("tweets_q6_7", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w8, _ := os.OpenFile("tweets_q6_8", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w9, _ := os.OpenFile("tweets_q6_9", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w10, _ := os.OpenFile("tweets_q6_10", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	w11, _ := os.OpenFile("tweets_q6_11", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w12, _ := os.OpenFile("tweets_q6_12", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w13, _ := os.OpenFile("tweets_q6_13", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w14, _ := os.OpenFile("tweets_q6_14", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w15, _ := os.OpenFile("tweets_q6_15", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	w16, _ := os.OpenFile("tweets_q6_16", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w17, _ := os.OpenFile("tweets_q6_17", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w18, _ := os.OpenFile("tweets_q6_18", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w19, _ := os.OpenFile("tweets_q6_19", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w20, _ := os.OpenFile("tweets_q6_20", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
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

	wmap[11] = w11
	wmap[12] = w12
	wmap[13] = w13
	wmap[14] = w14
	wmap[15] = w15

	wmap[16] = w16
	wmap[17] = w17
	wmap[18] = w18
	wmap[19] = w19
	wmap[20] = w20

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

	defer w11.Close()
	defer w12.Close()
	defer w13.Close()
	defer w14.Close()
	defer w15.Close()
	defer w16.Close()
	defer w17.Close()
	defer w18.Close()
	defer w19.Close()
	defer w20.Close()

	filepath.Walk("./file6", func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			split(path)
			fmt.Println("finished ", path)
		}
		return nil
	})
}
