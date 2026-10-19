// A web server that eats memory. It listens on $PORT, answers requests
// with how much it holds, and allocates 16 MiB more every second, so the
// runtime's memory limit can be watched doing its job: the instance is
// killed at the limit and the supervisor restarts it with backoff.
package main

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	var mu sync.Mutex
	var held [][]byte
	go func() {
		for {
			time.Sleep(time.Second)
			chunk := make([]byte, 16<<20)
			for i := 0; i < len(chunk); i += 4096 {
				chunk[i] = 1 // touch every page so the memory is really used
			}
			mu.Lock()
			held = append(held, chunk)
			n := len(held)
			mu.Unlock()
			fmt.Printf("memhog: holding %d MiB\n", n*16)
		}
	}()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := len(held)
		mu.Unlock()
		fmt.Fprintf(w, "memhog: holding %d MiB\n", n*16)
	})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
