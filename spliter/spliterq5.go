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
	wmap map[uint32]*os.File
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func split(path string) {
	r, err := os.Open(path)
	defer r.Close()

	if err != nil {
		panic(err.Error())
	}
	scanner := bufio.NewScanner(r)

	i := 0
	var buffer bytes.Buffer
	for scanner.Scan() {
		i++
		l := scanner.Text()
		vars := strings.Split(l, "\t")
		loc := hash(vars[0])%4 + 1
		w := wmap[loc]

		// vars[0] uid
		buffer.WriteString(vars[0] + "\t" + strings.Join(vars[1:], ",") + "\n")
		w.WriteString(buffer.String())
		buffer.Reset()
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
}

func main() {
	w1, _ := os.OpenFile("tweets_q5_1", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w2, _ := os.OpenFile("tweets_q5_2", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w3, _ := os.OpenFile("tweets_q5_3", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w4, _ := os.OpenFile("tweets_q5_4", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	wmap = make(map[uint32]*os.File)
	wmap[1] = w1
	wmap[2] = w2
	wmap[3] = w3
	wmap[4] = w4

	defer w1.Close()
	defer w2.Close()
	defer w3.Close()
	defer w4.Close()

	filepath.Walk("./file5", func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			split(path)
			fmt.Println("finished ", path)
		}
		return nil
	})
}
