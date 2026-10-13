package supervisor

import "time"

// Instance states as ps shows them.
const (
	StateRunning    = "running"    // the instance is up and routed
	StateStarting   = "starting"   // a deploy or restart is bringing one up
	StateRestarting = "restarting" // the instance exited; a restart is scheduled
	StateCrashed    = "crashed"    // too many exits; waiting for a manual restart
	StateStopped    = "stopped"    // nothing is running and nothing is scheduled
)

// Status is what the supervisor knows about an app's instance right now.
// With one instance per app it is one record; ps prints it.
type Status struct {
	App       string    `json:"app"`
	Release   string    `json:"release,omitempty"`  // vN of the instance, or of the release being started
	Instance  string    `json:"instance,omitempty"` // the runtime's id
	Addr      string    `json:"addr,omitempty"`
	State     string    `json:"state"`
	Restarts  int       `json:"restarts"`            // automatic restarts since the daemon started
	StartedAt time.Time `json:"started_at,omitzero"` // of the current instance
	Detail    string    `json:"detail,omitempty"`    // "exited with code 1; restart 3 in 4s", for people
}
