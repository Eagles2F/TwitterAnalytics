package main

import (
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type Prox struct {
	target *url.URL
	proxy  *httputil.ReverseProxy
}

var hosts map[int]*Prox

func (p *Prox) New(target string) {
	url, _ := url.Parse(target)
	p.target = url
	p.proxy = httputil.NewSingleHostReverseProxy(url)
}

func (p *Prox) handle(w http.ResponseWriter, r *http.Request) {
	p.proxy.ServeHTTP(w, r)
}

func rrhandler(w http.ResponseWriter, r *http.Request) {
	loc := rand.Intn(3) + 1
	hosts[loc].proxy.ServeHTTP(w, r)
}

func main() {
	hosts = make(map[int]*Prox)

	url1 := ""
	proxy1 := &Prox{}
	proxy1.New(url1)
	hosts[1] = proxy1

	url2 := ""
	proxy2 := &Prox{}
	proxy2.New(url2)
	hosts[2] = proxy2

	url3 := ""
	proxy3 := &Prox{}
	proxy3.New(url3)
	hosts[3] = proxy3

	http.HandleFunc("/q1", rrhandler)
	http.HandleFunc("/q2", rrhandler)

	http.ListenAndServe("80", nil)
}
