// A web server that dies. It listens at once, answers requests, and exits
// with status 1 after CRASH_AFTER (default 5s), so the supervisor's restart
// handling can be watched: restarts climbing with longer waits between them,
// then the app marked crashed once it has failed too often.
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
	after := 5 * time.Second
	if v := os.Getenv("CRASH_AFTER"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintln(os.Stderr, "CRASH_AFTER:", err)
			os.Exit(2)
		}
		after = d
	}
	go func() {
		time.Sleep(after)
		fmt.Fprintf(os.Stderr, "crasher: giving up after %s\n", after)
		os.Exit(1)
	}()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "crasher: alive, will exit %s after start\n", after)
	})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
