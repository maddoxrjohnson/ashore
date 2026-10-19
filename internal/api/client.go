package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/sse"
)

// Client calls the API. URL is the daemon's internal address with scheme,
// such as http://127.0.0.1:9000; Token is the admin token.
type Client struct {
	URL   string
	Token string
	// HTTP is the client used; nil means http.DefaultClient.
	HTTP *http.Client
}

// Config returns an app's config vars.
func (c *Client) Config(ctx context.Context, app string) (map[string]string, error) {
	var vars map[string]string
	err := c.getJSON(ctx, "/api/v1/apps/"+app+"/config", &vars)
	return vars, err
}

// Apps lists every app.
func (c *Client) Apps(ctx context.Context) ([]App, error) {
	var out []App
	err := c.getJSON(ctx, "/api/v1/apps", &out)
	return out, err
}

// App returns one app.
func (c *Client) App(ctx context.Context, name string) (App, error) {
	var out App
	err := c.getJSON(ctx, "/api/v1/apps/"+name, &out)
	return out, err
}

// CreateApp creates an app and its repository.
func (c *Client) CreateApp(ctx context.Context, name string) (App, error) {
	var out App
	err := c.postJSON(ctx, "/api/v1/apps", CreateAppRequest{Name: name}, &out)
	return out, err
}

// DestroyApp stops and removes an app.
func (c *Client) DestroyApp(ctx context.Context, name string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/api/v1/apps/"+name, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// Releases lists an app's releases, oldest first.
func (c *Client) Releases(ctx context.Context, app string) ([]Release, error) {
	var out []Release
	err := c.getJSON(ctx, "/api/v1/apps/"+app+"/releases", &out)
	return out, err
}

// Release returns one release by name ("v3").
func (c *Client) Release(ctx context.Context, app, version string) (Release, error) {
	var out Release
	err := c.getJSON(ctx, "/api/v1/apps/"+app+"/releases/"+version, &out)
	return out, err
}

// AddKey registers a public key (one authorized_keys line).
func (c *Client) AddKey(ctx context.Context, name, pubkey string) (Key, error) {
	var out Key
	err := c.postJSON(ctx, "/api/v1/keys", AddKeyRequest{Name: name, PubKey: pubkey}, &out)
	return out, err
}

// postJSON sends body and decodes the 2xx answer into v.
func (c *Client) postJSON(ctx context.Context, path string, body, v any) error {
	resp, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}

// Restart restarts the app's live release, streaming progress to out.
func (c *Client) Restart(ctx context.Context, app string, out io.Writer) error {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/apps/"+app+"/restart", struct{}{})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return readStream(resp.Body, out)
}

// Rollback makes a new release from version ("v3") and runs it.
func (c *Client) Rollback(ctx context.Context, app, version string, out io.Writer) error {
	resp, err := c.do(ctx, http.MethodPost, "/api/v1/apps/"+app+"/rollback", RollbackRequest{Version: version})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return readStream(resp.Body, out)
}

// PS returns the app's instances.
func (c *Client) PS(ctx context.Context, app string) (PS, error) {
	var out PS
	err := c.getJSON(ctx, "/api/v1/apps/"+app+"/ps", &out)
	return out, err
}

// UpdateConfig applies change and copies the redeploy's progress lines to
// out. The error is the deploy's, when it failed.
func (c *Client) UpdateConfig(ctx context.Context, app string, change ConfigChange, out io.Writer) error {
	resp, err := c.do(ctx, http.MethodPatch, "/api/v1/apps/"+app+"/config", change)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return readStream(resp.Body, out)
}

// getJSON decodes a 200 response into v.
func (c *Client) getJSON(ctx context.Context, path string, v any) error {
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}

// do sends one request with the token and returns a 2xx response; anything
// else is turned into an error carrying the server's message.
func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.URL, "/")+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("daemon at %s: %w", c.URL, err)
	}
	if resp.StatusCode/100 != 2 {
		defer func() { _ = resp.Body.Close() }()
		var e errorBody
		if json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(&e) != nil || e.Error == "" {
			e.Error = resp.Status
		}
		return nil, &Error{Status: resp.StatusCode, Message: e.Error}
	}
	return resp, nil
}

// Error is a non-2xx answer from the daemon.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return e.Message }

// readStream copies a progress stream's lines to out until its done or
// error event. A stream that ends without either is ErrStream.
func readStream(r io.Reader, out io.Writer) error {
	return readEvents(r, func(event, data string) error {
		if event == "" {
			_, err := fmt.Fprintln(out, data)
			return err
		}
		return nil
	})
}

// readEvents parses server-sent events and calls fn for each one that is
// neither the done nor the error event; those end the read. An error event
// is returned as an error; a stream that ends without a verdict is
// ErrStream. Comment lines are skipped.
func readEvents(r io.Reader, fn func(event, data string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if len(data) == 0 && event == "" {
				continue // a comment's blank line
			}
			text := strings.Join(data, "\n")
			switch event {
			case sse.EventDone:
				return nil
			case sse.EventError:
				return errors.New(text)
			default:
				if len(data) > 0 {
					if err := fn(event, text); err != nil {
						return err
					}
				}
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"):
			// a comment, such as a keepalive
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = append(data, strings.TrimPrefix(line, "data: "))
		case strings.HasPrefix(line, "data:"):
			data = append(data, "")
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return ErrStream
}

// Logs reads the app's recent lines and, with follow, keeps reading until
// ctx ends or the app is destroyed, calling fn for each line. tail is how
// many recent lines to start with.
func (c *Client) Logs(ctx context.Context, app string, tail int, follow bool, fn func(logs.Line) error) error {
	path := "/api/v1/apps/" + app + "/logs?tail=" + strconv.Itoa(tail)
	if follow {
		path += "&follow=1"
	}
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	err = readEvents(resp.Body, func(event, data string) error {
		if event != eventLine {
			return nil
		}
		var l logs.Line
		if err := json.Unmarshal([]byte(data), &l); err != nil {
			return fmt.Errorf("bad line event: %w", err)
		}
		return fn(l)
	})
	if ctx.Err() != nil {
		return nil // the caller stopped following; not an error
	}
	return err
}

// Is makes errors.Is(err, &Error{}) match any API error, and
// errors.Is(err, &Error{Status: 404}) match by status.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && (t.Status == 0 || t.Status == e.Status)
}
