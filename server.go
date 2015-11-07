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
	config       = Config{}
	db           *sql.DB
	query2Stmt1  *sql.Stmt
	query2Stmt2  *sql.Stmt
	query2Stmt3  *sql.Stmt
	query2Stmt4  *sql.Stmt
	query2Stmt5  *sql.Stmt
	query2Stmt6  *sql.Stmt
	query2Stmt7  *sql.Stmt
	query2Stmt8  *sql.Stmt
	query2Stmt9  *sql.Stmt
	query2Stmt10 *sql.Stmt

	stmtMap        map[uint32]*sql.Stmt
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
	stmt := queryStmt(uid)
	rows, err := stmt.Query(uid, timestamp)
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

func queryStmt(uid int64) *sql.Stmt {
	// the data set we get is ordered by uid and is string comparison
	uids := strconv.FormatInt(uid, 10)
	i := hash(uids)%10 + 1
	return stmtMap[i]
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

	// shared query1 prepared statement
	query2Stmt1, _ = db.Prepare("select tid, score, text from tweets_q2_1 where uid = ? and time = ?")
	query2Stmt2, _ = db.Prepare("select tid, score, text from tweets_q2_2 where uid = ? and time = ?")
	query2Stmt3, _ = db.Prepare("select tid, score, text from tweets_q2_3 where uid = ? and time = ?")
	query2Stmt4, _ = db.Prepare("select tid, score, text from tweets_q2_4 where uid = ? and time = ?")
	query2Stmt5, _ = db.Prepare("select tid, score, text from tweets_q2_5 where uid = ? and time = ?")
	query2Stmt6, _ = db.Prepare("select tid, score, text from tweets_q2_6 where uid = ? and time = ?")
	query2Stmt7, _ = db.Prepare("select tid, score, text from tweets_q2_7 where uid = ? and time = ?")
	query2Stmt8, _ = db.Prepare("select tid, score, text from tweets_q2_8 where uid = ? and time = ?")
	query2Stmt9, _ = db.Prepare("select tid, score, text from tweets_q2_9 where uid = ? and time = ?")
	query2Stmt10, _ = db.Prepare("select tid, score, text from tweets_q2_10 where uid = ? and time = ?")

	stmtMap = make(map[uint32]*sql.Stmt)
	stmtMap[1] = query2Stmt1
	stmtMap[2] = query2Stmt2
	stmtMap[3] = query2Stmt3
	stmtMap[4] = query2Stmt4
	stmtMap[5] = query2Stmt5
	stmtMap[6] = query2Stmt6
	stmtMap[7] = query2Stmt7
	stmtMap[8] = query2Stmt8
	stmtMap[9] = query2Stmt9
	stmtMap[10] = query2Stmt10

	if dbErr != nil {
		fmt.Println("error 0")
		panic(dbErr.Error())
	}
	defer query2Stmt1.Close()
	defer query2Stmt2.Close()
	defer query2Stmt3.Close()
	defer query2Stmt4.Close()
	defer query2Stmt5.Close()
	defer query2Stmt6.Close()
	defer query2Stmt7.Close()
	defer query2Stmt8.Close()
	defer query2Stmt9.Close()
	defer query2Stmt10.Close()

	http.HandleFunc("/index.html", index)
	http.HandleFunc("/q1", q1Handler)
	http.HandleFunc("/q2", q2Handler)
	http.HandleFunc("/q3", q3Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", config.HttpPort), nil)
}
