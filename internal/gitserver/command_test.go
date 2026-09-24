package gitserver

import "testing"

func TestParseCommand(t *testing.T) {
	ok := []struct{ raw, service, app string }{
		{"git-receive-pack 'hello'", "receive-pack", "hello"},
		{"git-upload-pack 'hello'", "upload-pack", "hello"},
		{"git-receive-pack '/hello'", "receive-pack", "hello"},
		{"git-receive-pack 'hello.git'", "receive-pack", "hello"},
		{"git-receive-pack '/hello.git'", "receive-pack", "hello"},
		{"git-receive-pack hello", "receive-pack", "hello"},
		{"git-receive-pack  'my-app-2' ", "receive-pack", "my-app-2"},
	}
	for _, tc := range ok {
		service, app, err := parseCommand(tc.raw)
		if err != nil || service != tc.service || app != tc.app {
			t.Errorf("parseCommand(%q) = %q, %q, %v; want %q, %q", tc.raw, service, app, err, tc.service, tc.app)
		}
	}

	bad := []string{
		"",
		"ls",
		"git-receive-pack",
		"git-receive-pack ''",
		"git receive-pack 'hello'",
		"git-receive-pack 'hello' extra",
		"git-receive-pack 'hello'; ls",
		"git-receive-pack 'a b'",
		"git-receive-pack '../etc'",
		"git-receive-pack '/'",
		"git-receive-pack 'Hello'",
		"git-receive-pack 'hello\\'",
		"git-receive-pack '.git'",
		"git-receive-pack 'abcdefghijklmnopqrstuvwxyz0123456789'",
	}
	for _, raw := range bad {
		if service, app, err := parseCommand(raw); err == nil {
			t.Errorf("parseCommand(%q) = %q, %q; want an error", raw, service, app)
		}
	}
}
