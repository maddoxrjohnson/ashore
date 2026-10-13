// A web server that takes its time: it sleeps before it listens, so a
// deploy that routed traffic the moment the container started would serve
// errors for that long. It also stops cleanly on SIGTERM, finishing requests
// in flight, which is the app's half of a zero-downtime swap. Every response
// carries the process start time, so a load generator can tell releases
// apart.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	delay := 10 * time.Second
	if v := os.Getenv("SLOWSTART_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintln(os.Stderr, "SLOWSTART_DELAY:", err)
			os.Exit(2)
		}
		delay = d
	}
	started := time.Now().UTC().Format(time.RFC3339Nano)
	fmt.Printf("sleeping %s before listening\n", delay)
	time.Sleep(delay)

	srv := &http.Server{Addr: ":" + port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "slowstart up since %s\n", started)
	})}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	fmt.Println("listening on", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
