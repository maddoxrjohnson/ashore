// A one-file web server for trying out config vars. It listens on $PORT and
// answers every request with the value of GREETING, so `config:set
// GREETING=hi` followed by a curl shows the new release running with the
// new environment.
package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		greeting := os.Getenv("GREETING")
		if greeting == "" {
			greeting = "(GREETING is not set)"
		}
		fmt.Fprintln(w, greeting)
	})
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
