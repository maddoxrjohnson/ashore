package site

import "testing"

func TestURLs(t *testing.T) {
	for _, tc := range []struct {
		site Site
		url  string
	}{
		{Site{HTTPPort: 8080}, "http://a.localhost:8080"},
		{Site{HTTPPort: 80}, "http://a.localhost"},
		{Site{}, "http://a.localhost"},
		{Site{HTTPPort: 80, HTTPSPort: 443}, "https://a.localhost"},
		{Site{HTTPPort: 8080, HTTPSPort: 8443}, "https://a.localhost:8443"},
	} {
		if got := tc.site.AppURL("a.localhost"); got != tc.url {
			t.Errorf("%+v: %q, want %q", tc.site, got, tc.url)
		}
	}
	s := Site{Domain: "example.test", SSHAddr: ":2222"}
	if s.AppHost("hello") != "hello.example.test" {
		t.Error("AppHost")
	}
	for addr, want := range map[string]string{
		":2222":          "ssh://ashore@example.test:2222/hello",
		"0.0.0.0:22":     "ssh://ashore@example.test:22/hello",
		"[::]:2222":      "ssh://ashore@example.test:2222/hello",
		"127.0.0.1:2299": "ssh://ashore@127.0.0.1:2299/hello",
		"bogus":          "ssh://ashore@example.test/hello",
	} {
		s.SSHAddr = addr
		if got := s.RemoteURL("hello"); got != want {
			t.Errorf("%s: %q, want %q", addr, got, want)
		}
	}
	for addr, want := range map[string]int{":8080": 8080, "127.0.0.1:9000": 9000, "x": 0, ":x": 0} {
		if PortOf(addr) != want {
			t.Errorf("PortOf(%q) = %d", addr, PortOf(addr))
		}
	}
}
