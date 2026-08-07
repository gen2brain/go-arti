// Command torprobe measures how fast and how concurrently the Tor pool can dial
// reachable public hosts, isolating Tor dial capacity from BitTorrent peer death.
//
//	go run ./examples/torprobe [instances] [concurrency]
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	arti "github.com/gen2brain/go-arti"
)

func main() {
	instances, concurrency := 2, 25
	if len(os.Args) > 1 {
		instances, _ = strconv.Atoi(os.Args[1])
	}
	if len(os.Args) > 2 {
		concurrency, _ = strconv.Atoi(os.Args[2])
	}

	targets := []string{
		"1.1.1.1:443", "8.8.8.8:443", "9.9.9.9:443", "1.0.0.1:443",
		"208.67.222.222:443", "8.8.4.4:443", "1.1.1.1:80", "8.8.8.8:80",
		"93.184.216.34:80", "151.101.0.81:443", "142.250.185.100:443", "140.82.121.4:443",
	}

	ctx := context.Background()
	log.Printf("bootstrapping %d Tor instances...", instances)
	p, err := arti.NewPool(ctx, "", instances)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	if err := p.Client().Bootstrap(); err != nil {
		log.Fatal("bootstrap: ", err)
	}
	log.Println("Tor ready; firing", concurrency, "concurrent dials")

	var wg sync.WaitGroup
	var mu sync.Mutex
	var ok, fail int
	var total time.Duration
	start := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addr := targets[i%len(targets)]
			t0 := time.Now()
			dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			conn, err := p.DialContext(dctx, "tcp", addr)
			d := time.Since(t0)
			mu.Lock()
			if err != nil {
				fail++
				fmt.Printf("  [%2d] FAIL %-22s %6dms %v\n", i, addr, d.Milliseconds(), err)
			} else {
				ok++
				total += d
				fmt.Printf("  [%2d] ok   %-22s %6dms\n", i, addr, d.Milliseconds())
				conn.Close()
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	avg := time.Duration(0)
	if ok > 0 {
		avg = total / time.Duration(ok)
	}
	log.Printf("RESULT: %d/%d ok in %s wall, avg connect %s (instances=%d concurrency=%d)",
		ok, concurrency, time.Since(start).Round(time.Millisecond), avg.Round(time.Millisecond), instances, concurrency)
}
