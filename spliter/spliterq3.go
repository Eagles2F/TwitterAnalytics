package main

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
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

	file, _ := ioutil.ReadFile(path)
	buf := bytes.NewBuffer(file)
	for {
		line, err := buf.ReadString('\n')
		if len(line) == 0 {
			if err != nil {
				if err == io.EOF {
					return
				}
			}
		}
		vars := strings.Split(line, "\t")

		w := wmap[hash(vars[0])%6+1]
		w.WriteString(line)
		if err != nil && err != io.EOF {
			fmt.Println("error happens")
			return
		}
	}
}

func main() {
	w1, _ := os.OpenFile("tweets_q3_1", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w2, _ := os.OpenFile("tweets_q3_2", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w3, _ := os.OpenFile("tweets_q3_3", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w4, _ := os.OpenFile("tweets_q3_4", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w5, _ := os.OpenFile("tweets_q3_5", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	w6, _ := os.OpenFile("tweets_q3_6", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)

	wmap = make(map[uint32]*os.File)
	wmap[1] = w1
	wmap[2] = w2
	wmap[3] = w3
	wmap[4] = w4
	wmap[5] = w5
	wmap[6] = w6

	defer w1.Close()
	defer w2.Close()
	defer w3.Close()
	defer w4.Close()
	defer w5.Close()
	defer w6.Close()

	filepath.Walk("./file3", func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			split(path)
			fmt.Println("finished ", path)
		}
		return nil
	})
}
