// A web server that talks. It prints a numbered line to stdout every
// second and a complaint to stderr every tenth second, so log streaming
// can be watched: `ashore logs -f chatty` and the dashboard should show
// each line within a moment of it being printed.
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	go func() {
		for i := 1; ; i++ {
			fmt.Printf("tick %d\n", i)
			if i%10 == 0 {
				fmt.Fprintf(os.Stderr, "tick %d: still nobody has asked me anything\n", i)
			}
			time.Sleep(time.Second)
		}
	}()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("request %s %s\n", r.Method, r.URL.Path)
		fmt.Fprintln(w, "chatty: see the logs")
	})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
