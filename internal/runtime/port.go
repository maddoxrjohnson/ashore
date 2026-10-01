package runtime

import (
	"fmt"
	"net"
)

// FreePort asks the kernel for an unused TCP port on 127.0.0.1.
//
// The port is free when FreePort returns, not reserved: between the Close
// here and the runtime binding it, another process could take it. That
// window is milliseconds wide, ashore is the only thing on the box handing
// out high ports, and deploys of one app run one at a time. A collision
// fails Start, which fails the deploy and leaves the old release serving,
// so the cost of losing the race is a retried push, not an outage.
func FreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate port: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, fmt.Errorf("allocate port: %w", err)
	}
	return port, nil
}
