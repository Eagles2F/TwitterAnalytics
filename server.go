package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"time"
	"unicode/utf8"
)

type Config struct {
	TeamId           string
	TeamAwsAccountId string
	SecretKey        string
	HttpPort         int
}

var config = Config{}

func initConfig() {
	file, _ := os.Open("config.json")
	err := json.NewDecoder(file).Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}
	file.Close()
}

func q1Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	key := r.URL.Query().Get("key")
	message := r.URL.Query().Get("message")
	// Time
	now := time.Now()
	response := fmt.Sprintf("%s,%s\n%s\n%s", config.TeamId, config.TeamAwsAccountId,
		now.Format("2006-01-02 15:04:05"), decipher(message, key))
	io.WriteString(w, response)
}

func decipher(message string, key string) string {
	//calculate key Y
	sk := big.NewInt(0)
	sk.SetString(config.SecretKey, 10)
	k := big.NewInt(0)
	k.SetString(key, 10)
	var y big.Int
	y.Div(k, sk)
	n := int(math.Sqrt(float64(utf8.RuneCountInString(message))))

	//de-diagonalize
	var buffer bytes.Buffer

	for i := 0; i < 2*n-1; i++ {
		var z int
		if i < n {
			z = 0
		} else {
			z = i - n + 1
		}
		for j := z; j <= i-z; j++ {
			buffer.WriteString(string(message[j*n+i-j]))
		}
	}

	intermediate := buffer.String()

	//shift back
	yn := y.Int64()
	z := yn%25 + 1
	zint := int(z)
	var buffer1 bytes.Buffer
	for i := 0; i < len(intermediate); i++ {
		order := int(intermediate[i] - 'A')
		if order < zint {
			buffer1.WriteString(string('Z' - (zint - order - 1)))
		} else {
			buffer1.WriteString(string(int(intermediate[i]) - zint))
		}
	}
	return buffer1.String()
}

func main() {
	initConfig()
	http.HandleFunc("/q1", q1Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", config.HttpPort), nil)
}
