package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
)

type Prox struct {
	target *url.URL
	proxy  *httputil.ReverseProxy
}

func (p *Prox) New(target string) {
	url, _ := url.Parse(target)
	p.target = url
	p.proxy = httputil.NewSingleHostReverseProxy(url)
}

func (p *Prox) handle(w http.ResponseWriter, r *http.Request) {
	p.proxy.ServeHTTP(w, r)
}

func main() {
	const (
		defaultPort        = ":80"
		defaultPortUsage   = "default server port, ':80', ':80'..."
		defaultTarget      = "http://ec2-54-85-196-129.compute-1.amazonaws.com/"
		defaultTargetUsage = "default redirect url, 'http://ec2-54-85-196-129.compute-1.amazonaws.com/'"
	)

	// flags
	port := flag.String("port", defaultPort, defaultPortUsage)
	url := flag.String("url", defaultTarget, defaultTargetUsage)

	flag.Parse()

	fmt.Println("server will run on : %s", *port)
	fmt.Println("redirecting to :%s", *url)

	proxy := &Prox{}
	proxy.New(*url)
	http.HandleFunc("/index.html", proxy.handle)

	http.ListenAndServe(*port, nil)
}
