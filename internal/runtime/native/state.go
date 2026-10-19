// What the runtime remembers about an instance on disk, and how it tells
// whether the process is still the one it remembers.
package native

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// state is what the runtime remembers about an instance on disk, so a
// restarted daemon can find it again.
type state struct {
	ID        string            `json:"id"`
	App       string            `json:"app"`
	Release   string            `json:"release"`
	Image     string            `json:"image"`
	Pid       int               `json:"pid"`
	StartTime uint64            `json:"start_time"` // the kernel's, from /proc/<pid>/stat, against pid reuse
	Port      int               `json:"port"`
	Cgroup    string            `json:"cgroup,omitempty"` // the instance's cgroup directory, when limits are enforced
	CreatedAt time.Time         `json:"created_at"`
	Labels    map[string]string `json:"labels,omitempty"`
}

func writeState(dir string, st state) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "state.json.tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "state.json"))
}

func readState(dir string) (state, error) {
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return state{}, err
	}
	var st state
	if err := json.Unmarshal(data, &st); err != nil {
		return state{}, fmt.Errorf("%s: %w", dir, err)
	}
	return st, nil
}

// startTime is field 22 of /proc/<pid>/stat: when the process started, in
// clock ticks since boot. A pid can be reused; a pid with this start time
// cannot.
func startTime(pid int) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	// The command name is in parentheses and may contain spaces; fields
	// are counted after the closing one.
	i := strings.LastIndexByte(string(data), ')')
	if i < 0 {
		return 0, fmt.Errorf("/proc/%d/stat: unexpected format", pid)
	}
	fields := strings.Fields(string(data[i+1:]))
	if len(fields) < 20 {
		return 0, fmt.Errorf("/proc/%d/stat: unexpected format", pid)
	}
	return strconv.ParseUint(fields[19], 10, 64) // field 22 overall; 20th after the name
}

// alive reports whether the process the state describes still exists,
// start time included, so a reused pid is not mistaken for it.
func alive(st state) bool {
	if st.Pid <= 0 {
		return false
	}
	t, err := startTime(st.Pid)
	return err == nil && (st.StartTime == 0 || t == st.StartTime)
}
