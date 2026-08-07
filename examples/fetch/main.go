// Fetch clearnet pages (http + https) and connect to a .onion service, all over embedded Tor.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	arti "github.com/gen2brain/go-arti"
)

func main() {
	ctx := context.Background()

	c, err := arti.New(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	fmt.Println("bootstrapping Tor (downloads a consensus; give it a minute)...")
	t0 := time.Now()
	if err := c.Bootstrap(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bootstrapped in %s\n\n", time.Since(t0).Round(time.Second))

	client := c.HTTPClient()

	// Clearnet HTTP and HTTPS (TLS is performed by Go over the Tor stream).
	show(client.Get("http://example.com/"))
	show(client.Get("https://example.com/"))

	// A .onion hidden service (raw stream; the Tor Project onion, port 80).
	onion := "2gzyxa5ihm7nsggfxnu52rck2vv4rvmdlkiu3zzui5du4xyclen53wid.onion"
	conn, err := c.Dial("tcp", onion+":80")
	if err != nil {
		log.Fatalf("onion dial: %v", err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET / HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n", onion)
	status, _ := bufio.NewReader(conn).ReadString('\n')
	fmt.Printf("%-45s -> %s", onion[:20]+"....onion", status)
}

func show(resp *http.Response, err error) {
	if err != nil {
		fmt.Println("  request failed:", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 120))
	fmt.Printf("%-45s -> %s  %s...\n", resp.Request.URL, resp.Status,
		strings.TrimSpace(string(body[:min(len(body), 60)])))
}
