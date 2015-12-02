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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
	"sync"
)

type Config struct {
	TeamId           string
	TeamAwsAccountId string
	SecretKey        string
	HttpPort         int
}

type Q3Response struct {
	Timestamp int
	Score     int
	TweetId   int64
	Text      string
}

type Q4Response struct {
	Count     int
	Timestamp int
	Content   string
}

type Q6Operation struct {
	seq int
	tweetid string
	tag string
	operation_type string //a for appending, r for reading
}

type Q6Transaction struct {
	start_arrived bool
	end_arrived bool
	opts []Q6Operation
}

//priority queue for Q6Operation
type Q6Queue []Q6Operation

func (pq Q6Queue) Len() int { return len(pq) }

func (pq Q6Queue) Less(i, j int) bool {
	return pq[i].seq < pq[j].seq
}

func (pq Q6Queue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

// end of priority queue section

// positive/negative tweets
type PNTweets []Q3Response

// hashtag tweets
type HTTweets []Q4Response

// for comparison
func (t PNTweets) Len() int      { return len(t) }
func (t PNTweets) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t PNTweets) Less(i, j int) bool {
	if math.Abs(float64(t[i].Score)) == math.Abs(float64(t[j].Score)) {
		return t[i].TweetId < t[j].TweetId
	} else {
		return math.Abs(float64(t[i].Score)) > math.Abs(float64(t[j].Score))
	}
}

// for comparison
func (t HTTweets) Len() int      { return len(t) }
func (t HTTweets) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t HTTweets) Less(i, j int) bool {
	if t[i].Count == t[j].Count {
		return t[i].Timestamp < t[j].Timestamp
	} else {
		return t[i].Count > t[j].Count
	}
}

var (
	config         = Config{}
	db             *sql.DB
	qtable         map[string]*sql.Stmt
	responseHeader string
	transactionMap map[int]*Q6Transaction
	mutex *sync.Mutex
	writeLogMap    map[string]string
	writeLock *sync.Mutex
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

func (response *Q3Response) String() string {
	tm := time.Unix(int64(response.Timestamp), 0)
	// Mon Jan 2 15:04:05 -0700 MST 2006
	strTime := tm.Format("2006-01-02")
	return fmt.Sprintf("%s,%d,%d,%s\n", strTime, response.Score, response.TweetId, response.Text)
}

func (response *Q4Response) String() string {
	tm := time.Unix(int64(response.Timestamp), 0)
	// Mon Jan 2 15:04:05 -0700 MST 2006
	strTime := tm.Format("2006-01-02")
	return fmt.Sprintf("%s:%d:%s\n", strTime, response.Count, response.Content)
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
	// HACK: I made a mistake when extracting and loading data
	// and I have to use this same ugly stupid code snippet to make it correct
	// following is supposed to the correct one
	//	rs := query2(uid, strconv.FormatInt(timestamp, 10))
	st := strconv.FormatInt(timestamp, 10)
	it, _ := strconv.ParseFloat(st, 32)
	st = strconv.Itoa((int)(it))
	rs := query2(uid, st)

	body := fmt.Sprintf("%s%s", responseHeader, rs)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func q3Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	start := r.URL.Query().Get("start_date")
	end := r.URL.Query().Get("end_date")
	uid := r.URL.Query().Get("userid")
	n := r.URL.Query().Get("n")

	// convert to numerical values
	n32, _ := strconv.Atoi(n)
	fts, _ := time.Parse("2006-01-02", start)
	fte, _ := time.Parse("2006-01-02", end)
	// format date to epoch timestamp
	startu := fts.Unix()
	endu := fte.Unix()
	rs := query3(uid, int(startu), int(endu), n32)

	body := fmt.Sprintf("%s%s", responseHeader, rs)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func q4Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	hashtag := r.URL.Query().Get("hashtag")
	n := r.URL.Query().Get("n")

	// convert to numerical values
	n32, _ := strconv.Atoi(n)
	// format date to epoch timestamp
	rs := query4(hashtag, n32)

	body := fmt.Sprintf("%s%s", responseHeader, rs)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func q5Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	min_uid := r.URL.Query().Get("userid_min")
	max_uid := r.URL.Query().Get("userid_max")

	rs := query5(min_uid, max_uid)
	body := fmt.Sprintf("%s%s", responseHeader, rs)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func q6Handler(w http.ResponseWriter, r *http.Request) {
	// Get parameters
	opt := r.URL.Query().Get("opt") //operation type
	tid := r.URL.Query().Get("tid") //transaction ID
	var tag string
	var seq string
	var tweetid string
	if opt == "a" {
		seq = r.URL.Query().Get("seq") //sequence number
		tweetid = r.URL.Query().Get("tweetid") //tweet ID that you need to append to
		tag = r.URL.Query().Get("tag") //the string that you will append at the end of the tweet text
	} else if opt == "r" {
		seq = r.URL.Query().Get("seq") //sequence number
		tweetid = r.URL.Query().Get("tweetid") //tweet ID that you need to append to
	}

	//collect transactions with a Map from transaction ID to transaction
	tid32, _ := strconv.Atoi(tid)
	seq32, _ := strconv.Atoi(seq)

	mutex.Lock()
	if _, ok:= transactionMap[tid32]; !ok {
		transactionMap[tid32]= &Q6Transaction{}
	}
	mutex.Unlock()

	var rs string

	if opt == "s" {
		mutex.Lock()
		transactionMap[tid32].start_arrived = true
		mutex.Unlock()
		rs = "0\n"
	} else if opt == "a" {
		operation := Q6Operation{seq:seq32, tweetid:tweetid, tag:tag, operation_type:"a"}
		mutex.Lock()

		transactionMap[tid32].opts = append(transactionMap[tid32].opts, operation)
		sort.Sort(Q6Queue(transactionMap[tid32].opts))

		mutex.Unlock()
		//write should be blocking until it is the first one in the queue
		//then proceed the requests by the order of seq number
		var s int
		mutex.Lock()
		s = transactionMap[tid32].opts[0].seq
		mutex.Unlock()
		for  s != seq32 {
			time.Sleep(time.Millisecond)
			mutex.Lock()
			s = transactionMap[tid32].opts[0].seq
			mutex.Unlock()
		}
		//do the write on a goroutine
		go query6write(tweetid, tag)
		rs = fmt.Sprintf("%s\n", tag)
		mutex.Lock()
		transactionMap[tid32].opts = transactionMap[tid32].opts[1:]
		mutex.Unlock()
	} else if opt  == "r" {
		operation := Q6Operation{seq:seq32, tweetid:tweetid, operation_type:"r"}
		mutex.Lock()

		transactionMap[tid32].opts = append(transactionMap[tid32].opts, operation)
		sort.Sort(Q6Queue(transactionMap[tid32].opts))

		mutex.Unlock()
		//read should be blocking until it is the first one in the queue
		//then proceed the requests by the order of seq number

		var s int
		mutex.Lock()
		s = transactionMap[tid32].opts[0].seq
		mutex.Unlock()
		for  s != seq32 {
			time.Sleep(time.Millisecond)
			mutex.Lock()
			s = transactionMap[tid32].opts[0].seq
			mutex.Unlock()
		}

		rs = fmt.Sprintf("%s\n", query6read(tweetid))//should be the result of query
		mutex.Lock()
		transactionMap[tid32].opts = transactionMap[tid32].opts[1:]
		mutex.Unlock()
	} else if opt == "e" {
		mutex.Lock()
		transactionMap[tid32].end_arrived = true
		mutex.Unlock()
		rs = "0\n"
	}

	body := fmt.Sprintf("%s%s", responseHeader, rs)
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	io.WriteString(w, body)
}

func query2(uid string, timestamp string) string {
	var content string
	var buffer bytes.Buffer
	stmt := getQueryStmt("2", uid)
	err := stmt.QueryRow(uid + "," + timestamp).Scan(&content)
	if err != nil {
		return ""
	}
	rs := strings.Replace(content, ",", ":", 2)
	buffer.WriteString(unescape(rs) + "\n")
	return buffer.String()
}

func query3(uid string, start int, end int, limit int) string {
	var contents string
	stmt := getQueryStmt("3", uid)
	uid64, _ := strconv.ParseInt(uid, 10, 64)
	err := stmt.QueryRow(uid64).Scan(&contents)
	if err != nil {
		return ""
	}
	var buffer bytes.Buffer
	var responses []Q3Response

	buffer.WriteString("Positive Tweets\n")
	// timestamp, score, tid, text
	response := Q3Response{}
	tweets := strings.Split(contents, "[####&&&&]")

	for idx := range tweets {
		fields := strings.Split(tweets[idx], "(@@@@****)")
		timef, _ := strconv.ParseFloat(fields[1], 64)
		timei := int(timef)
		if timei >= start && timei <= end {
			response.Timestamp = timei
			response.Score, _ = strconv.Atoi(fields[3])
			response.TweetId, _ = strconv.ParseInt(fields[0], 10, 64)
			response.Text = fields[2]
			responses = append(responses, response)
		}
	}

	sort.Sort(PNTweets(responses))

	var pcount, ncount int
	pcount = 0
	ncount = 0
	var negs []Q3Response
	for idx := range responses {
		if responses[idx].Score > 0 && pcount < limit {
			buffer.WriteString(responses[idx].String())
			pcount++
		}
		if responses[idx].Score < 0 && ncount < limit {
			negs = append(negs, responses[idx])
			ncount++
		}
		if pcount >= limit && ncount >= limit {
			break
		}
	}
	buffer.WriteString("\nNegative Tweets\n")
	for idx := range negs {
		buffer.WriteString(negs[idx].String())
	}
	return buffer.String()
}

func query4(hashtag string, limit int) string {
	stmt := getQueryStmt("4", hashtag)

	var buffer bytes.Buffer
	var responses []Q4Response
	var content string
	err := stmt.QueryRow(hashtag).Scan(&content)
	if err != nil {
		return ""
	}
	tweets := strings.SplitN(content, "asgdhjbf673bvsalfjoq3ng", -1)

	for idx := range tweets {
		resp := Q4Response{}
		cols := strings.SplitN(tweets[idx], ":", 3)
		tsi, _ := time.Parse("2006-01-02", cols[0])
		resp.Timestamp = int(tsi.Unix())
		resp.Count, _ = strconv.Atoi(cols[1])
		resp.Content = cols[2]
		responses = append(responses, resp)
	}
	sort.Sort(HTTweets(responses))

	for idx := range responses {
		if idx < limit {
			buffer.WriteString(responses[idx].String())
		} else {
			break
		}
	}
	return buffer.String()
}

func query5(min_uid string, max_uid string) string {
	// convert to numerical values
	id_min_64, _ := strconv.ParseInt(min_uid, 10, 64)
	id_max_64, _ := strconv.ParseInt(max_uid, 10, 64)

	var content string
	total_count := 0

	min_search := true
	for min_search {
		min_uid = strconv.FormatInt(id_min_64, 10)
		stmt := getQueryStmt("5", min_uid)

		err := stmt.QueryRow(id_min_64).Scan(&content)
		if err != nil {
			fmt.Println("first err ", err, " id_min_64, ", id_min_64, ", max_64 ", id_max_64)
			id_min_64 += 1
		} else {
			min_search = false
		}
	}
	min_counts := strings.Split(content, ",")

	max_search := true
	for max_search {
		max_uid = strconv.FormatInt(id_max_64, 10)
		stmt := getQueryStmt("5", max_uid)

		err := stmt.QueryRow(id_max_64).Scan(&content)
		if err != nil {
			fmt.Println("second err ", err, " id_min_64, ", id_min_64, ", max_64 ", id_max_64)
			id_max_64 -= 1
		} else {
			max_search = false
		}
	}
	max_counts := strings.Split(content, ",")
	max_elapsed_count, _ := strconv.Atoi(max_counts[1])
	min_elapsed_count, _ := strconv.Atoi(min_counts[1])
	min_uid_count, _ := strconv.Atoi(min_counts[0])
	total_count = max_elapsed_count - min_elapsed_count + min_uid_count
	return strconv.Itoa(total_count) + "\n"
}

func query6read(tid string) string {
	//tid_64, _ := strconv.ParseInt(tid, 10, 64)
	stmt := getQueryStmt("6read", tid)
	if stmt == nil {
		fmt.Printf("stmt nil")
	}
	var content string
	err := stmt.QueryRow(tid).Scan(&content)
	if err != nil {
		return ""
	}

	writeLock.Lock()
	if _, ok:= writeLogMap[tid]; ok {
		 content = fmt.Sprintf("%s%s", content, writeLogMap[tid])
	}
	writeLock.Unlock()

	return content
}

func query6write(tweetid string, tag string) {
	//append to tag
	writeLock.Lock()
	writeLogMap[tweetid] = tag
	writeLock.Unlock()
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

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func getQueryStmt(prefix string, key string) *sql.Stmt {
	// the data set we get is ordered by uid and is string comparison
	var i int
	if prefix == "5" {
		i = (int)(hash(key)%4 + 1)
	} else if prefix == "4" {
		i = (int)(hash(key)%3 + 1)
	} else if prefix == "3" {
		i = (int)(hash(key)%6 + 1)
	} else if prefix == "2" {
		i = (int)(hash(key)%10 + 1)
	} else {
		i = (int)(hash(key)%20 + 1)
	}
	return qtable[prefix+strconv.Itoa(i)]
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
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)
	if err != nil {
		panic(err.Error())
	}
	return db
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	initConfig()
	responseHeader = fmt.Sprintf("%s,%s\n", config.TeamId, config.TeamAwsAccountId)

	mutex = &sync.Mutex{}

	writeLock = &sync.Mutex{}

	// shared database connection
	db = getDbConn()
	defer db.Close()

	writeLogMap = make(map[string]string)

	qtable = make(map[string]*sql.Stmt)
	prefix2 := "2"
	prefix3 := "3"
	prefix4 := "4"
	prefix5 := "5"
	prefix6read := "6read"
	for i := 1; i < 11; i++ {
		qtable[prefix2+strconv.Itoa(i)], _ = db.Prepare("select tidst from tweets_q2_" + strconv.Itoa(i) + " where uidt = ? limit 1")
	}
	for i := 1; i < 7; i++ {
		qtable[prefix3+strconv.Itoa(i)], _ = db.Prepare("select content from tweets_q3_" + strconv.Itoa(i) + " where uid = ? limit 1")
	}
	for i := 1; i < 4; i++ {
		qtable[prefix4+strconv.Itoa(i)], _ = db.Prepare("select content from tweets_q4_" + strconv.Itoa(i) + " where tag = ? limit 1")
	}
	for i := 1; i < 5; i++ {
		qtable[prefix5+strconv.Itoa(i)], _ = db.Prepare("select counts from tweets_q5_" + strconv.Itoa(i) + " where uid = ? limit 1")
	}
	for i := 1; i < 21; i++ {
		qtable[prefix6read+strconv.Itoa(i)], _ = db.Prepare("select tweet from tweets_q6_" + strconv.Itoa(i) + " where tid = ? limit 1")
	}


	//initialize the map
	transactionMap = make(map[int]*Q6Transaction)

	http.HandleFunc("/index.html", index)
	http.HandleFunc("/q1", q1Handler)
	http.HandleFunc("/q2", q2Handler)
	http.HandleFunc("/q3", q3Handler)
	http.HandleFunc("/q4", q4Handler)
	http.HandleFunc("/q5", q5Handler)
	http.HandleFunc("/q6", q6Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", config.HttpPort), nil)
}
