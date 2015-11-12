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
	// uid timestamp (float) tid text score
	for scanner.Scan() {
		i++
		l := scanner.Text()
		vars := strings.Split(l, "\t")
		loc := hash(vars[0])%3 + 1
		w := wmap[loc]
		buffer.WriteString(l + "\n")
		w.WriteString(buffer.String())
		buffer.Reset()
	}
	fmt.Printf("line number %d\n", i)
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
}

func main() {
	w1, _ := os.OpenFile("tweets_q4_1", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w2, _ := os.OpenFile("tweets_q4_2", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w3, _ := os.OpenFile("tweets_q4_3", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	wmap = make(map[uint32]*os.File)
	wmap[1] = w1
	wmap[2] = w2
	wmap[3] = w3

	defer w1.Close()
	defer w2.Close()
	defer w3.Close()
	filepath.Walk("./file4", func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			split(path)
			fmt.Println("finished ", path)
		}
		return nil
	})
}
