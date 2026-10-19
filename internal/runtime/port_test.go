package runtime

import (
	"net"
	"strconv"
	"testing"
)

func TestFreePortCanBeBound(t *testing.T) {
	port, err := FreePort()
	if err != nil {
		t.Fatal(err)
	}
	if port < 1024 || port > 65535 {
		t.Fatalf("port %d outside the unprivileged range", port)
	}
	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatalf("port %d not bindable after FreePort: %v", port, err)
	}
	_ = l.Close()
}

func TestFreePortSkipsPortsInUse(t *testing.T) {
	// Hold a set of ports open; FreePort must never return one of them.
	held := map[int]bool{}
	for range 20 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		held[l.Addr().(*net.TCPAddr).Port] = true
	}
	for range 50 {
		port, err := FreePort()
		if err != nil {
			t.Fatal(err)
		}
		if held[port] {
			t.Fatalf("FreePort returned %d, which is in use", port)
		}
	}
}

func TestSpecValidateAndLabels(t *testing.T) {
	s := Spec{App: "a", Release: "v1", HostPort: 4000, ContainerPort: 8080, Labels: map[string]string{"x": "1", LabelApp: "hidden"}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Spec{{HostPort: 0, ContainerPort: 1}, {HostPort: 70000, ContainerPort: 1}, {HostPort: 1, ContainerPort: 0}} {
		if bad.Validate() == nil {
			t.Errorf("%+v should be invalid", bad)
		}
	}
	l := s.AllLabels()
	if l["x"] != "1" || l[LabelApp] != "a" || l[LabelRelease] != "v1" || len(l) != 3 {
		t.Fatalf("labels %v", l)
	}
	if Short("0123456789abcdef") != "0123456789ab" || Short("ab") != "ab" {
		t.Error("Short")
	}
}
