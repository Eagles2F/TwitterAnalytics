package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"hash/fnv"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type Config struct {
	TeamId           string
	TeamAwsAccountId string
	SecretKey        string
	HttpPort         int
}

var (
	config         = Config{}
	db             *sql.DB
	utable         map[uint32]string
	dbErr          error
	responseHeader string
)

func initConfig() {
	file, err := os.Open("config.json")
	if err != nil {
		fmt.Println("error:", err)
	}
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		fmt.Println("error:", err)
	}
	file.Close()
}

func index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", "0")
	io.WriteString(w, "")
}

func q1Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	key := r.URL.Query().Get("key")
	message := r.URL.Query().Get("message")
	// Time
	now := time.Now()
	body := fmt.Sprintf("%s%s\n%s\n", responseHeader,
		now.Format("2006-01-02 15:04:05"), decipher(message, key))
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func q2Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	uid := r.URL.Query().Get("userid")
	tm := r.URL.Query().Get("tweet_time")
	ft, _ := time.Parse("2006-01-02 15:04:05", tm)
	// format date to epoch timestamp
	timestamp := ft.Unix()
	uid64, _ := strconv.ParseInt(uid, 10, 64)
	rs := query2(uid64, timestamp)
	body := fmt.Sprintf("%s%s", responseHeader, rs)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func q3Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	/*
		start := r.URL.Query().Get("start_date")
		end := r.URL.Query().Get("end_date")
		uid := r.URL.Query().Get("userid")
		n := r.URL.Query().Get("n")

		// convert to numerical values
		uid64, _ := strconv.ParseInt(uid, 10, 64)
		n32, _ := strconv.Atoi(n)
		fts, _ := time.Parse("2006-01-02", start)
		fte, _ := time.Parse("2006-01-02", end)
		// format date to epoch timestamp
		startu := fts.Unix()
		endu := fte.Unix()

		rs := query3(startu, endu, uid64, n32)
		body := fmt.Sprintf("%s%s", responseHeader, rs)
		w.Header().Set("Content-Type", "text/plain;charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		io.WriteString(w, body)
	*/
}

func query2(uid int64, timestamp int64) string {
	tb := uidTable(uid)
	q := fmt.Sprint("select tid, score, text from ", tb, " where uid = ? and time = ?")
	rows, err := db.Query(q, uid, timestamp)
	if err != nil {
		panic(err.Error())
	}
	defer rows.Close()
	var tid int64
	var score int
	var text string
	var buffer bytes.Buffer
	// tid, score, text
	for rows.Next() {
		if err := rows.Scan(&tid, &score, &text); err != nil {
			panic(err.Error())
		}
		buffer.WriteString(unescape(fmt.Sprintf("%d:%d:%s\n", tid, score, text)))
	}
	if err := rows.Err(); err != nil {
		panic(err.Error())
	}
	return buffer.String()
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func uidTable(uid int64) string {
	// the data set we get is ordered by uid and is string comparison
	uids := strconv.FormatInt(uid, 10)
	i := hash(uids)%10 + 1
	return utable[i]
}

func query3(start int64, end int64, uid int64, n int) string {
	return ""
}

func unescape(line string) string {
	line = strings.Replace(line, "\\n", "\n", -1)
	line = strings.Replace(line, "\\r", "\r", -1)
	line = strings.Replace(line, "\\t", "\t", -1)
	line = strings.Replace(line, "\\f", "\f", -1)
	line = strings.Replace(line, "\\b", "\b", -1)
	line = strings.Replace(line, "\\\"", "\"", -1)
	line = strings.Replace(line, "\\\\", "\\", -1)
	return line
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

func getDbConn() *sql.DB {
	db, err := sql.Open("mysql", "root:@/purrito")
	if err != nil {
		panic(err.Error())
	}
	return db
}

func main() {
	initConfig()
	responseHeader = fmt.Sprintf("%s,%s\n", config.TeamId, config.TeamAwsAccountId)

	// shared database connection
	db = getDbConn()
	defer db.Close()

	utable = make(map[uint32]string)
	utable[0] = "tweets_q2_1"
	utable[1] = "tweets_q2_1"
	utable[2] = "tweets_q2_2"
	utable[3] = "tweets_q2_3"
	utable[4] = "tweets_q2_4"
	utable[5] = "tweets_q2_5"
	utable[6] = "tweets_q2_6"
	utable[7] = "tweets_q2_7"
	utable[8] = "tweets_q2_8"
	utable[9] = "tweets_q2_9"
	utable[10] = "tweets_q2_10"

	if dbErr != nil {
		fmt.Println("error 0")
		panic(dbErr.Error())
	}
	http.HandleFunc("/index.html", index)
	http.HandleFunc("/q1", q1Handler)
	http.HandleFunc("/q2", q2Handler)
	http.HandleFunc("/q3", q3Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", config.HttpPort), nil)
}
