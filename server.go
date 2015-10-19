package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
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
	fmt.Println(config)
}

func q1Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	key := r.URL.Query().Get("key")
	message := r.URL.Query().Get("message")
	// Time
	now := time.Now()
	response := fmt.Sprintf("%s,%s\n%s\n%s", config.TeamId, config.TeamAwsAccountId,
		now.Format("2006-01-02 15:04:05"), decipher(key, message))
	io.WriteString(w, response)
}

func decipher(product string, cipher string) string {
	return ""
}

func main() {
	initConfig()
	http.HandleFunc("/q1", q1Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", config.HttpPort), nil)
}
