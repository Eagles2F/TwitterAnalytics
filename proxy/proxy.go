package main

import (
	"hash/fnv"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type Prox struct {
	target *url.URL
	proxy  *httputil.ReverseProxy
}

var hosts map[uint32]*Prox

func (p *Prox) New(target string) {
	url, _ := url.Parse(target)
	p.target = url
	p.proxy = httputil.NewSingleHostReverseProxy(url)
}

func (p *Prox) handle(w http.ResponseWriter, r *http.Request) {
	p.proxy.ServeHTTP(w, r)
}

func rrhandler(w http.ResponseWriter, r *http.Request) {
	loc := uint32(rand.Intn(8) + 1)
	hosts[loc].proxy.ServeHTTP(w, r)
}

func chhandler(w http.ResponseWriter, r *http.Request) {
	tid := r.URL.Query().Get("tid")
	loc := hash(tid)%8 + 1
	hosts[loc].proxy.ServeHTTP(w, r)
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func main() {
	hosts = make(map[uint32]*Prox)

	url1 := "http://ec2-54-172-210-69.compute-1.amazonaws.com"
	proxy1 := &Prox{}
	proxy1.New(url1)
	hosts[1] = proxy1

	url2 := "http://ec2-54-172-162-149.compute-1.amazonaws.com"
	proxy2 := &Prox{}
	proxy2.New(url2)
	hosts[2] = proxy2

	url3 := "http://ec2-54-174-16-214.compute-1.amazonaws.com"
	proxy3 := &Prox{}
	proxy3.New(url3)
	hosts[3] = proxy3

	url4 := "http://ec2-54-173-108-98.compute-1.amazonaws.com"
	proxy4 := &Prox{}
	proxy4.New(url4)
	hosts[4] = proxy4

	url5 := "http://ec2-54-173-1-161.compute-1.amazonaws.com"
	proxy5 := &Prox{}
	proxy5.New(url5)
	hosts[5] = proxy5

	url6 := "http://ec2-54-86-36-248.compute-1.amazonaws.com"
	proxy6 := &Prox{}
	proxy6.New(url6)
	hosts[6] = proxy6

	url7 := "http://ec2-54-172-229-95.compute-1.amazonaws.com"
	proxy7 := &Prox{}
	proxy7.New(url7)
	hosts[7] = proxy7

	url8 := "http://ec2-54-88-59-148.compute-1.amazonaws.com"
	proxy8 := &Prox{}
	proxy8.New(url8)
	hosts[8] = proxy8

	http.HandleFunc("/q1", rrhandler)
	http.HandleFunc("/q2", rrhandler)
	http.HandleFunc("/q3", rrhandler)
	http.HandleFunc("/q4", rrhandler)
	http.HandleFunc("/q5", rrhandler)
	http.HandleFunc("/q6", chhandler)
	http.ListenAndServe(":80", nil)
}
