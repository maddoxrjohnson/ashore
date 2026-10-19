// What the supervisor reports about itself. The metrics package
// implements Observer; the default discards everything.
package supervisor

import (
	"time"
)

// Observer is told what the supervisor does; the metrics package implements
// it and the default discards everything.
type Observer interface {
	// Deploy counts one finished deploy: ResultLive or ResultFailed.
	Deploy(app, result string)
	// Stage records how long a deploy stage took: one of the Stage constants.
	Stage(stage string, d time.Duration)
	// InstanceState reports the app's instance state as ps shows it.
	InstanceState(app, state string)
	// InstanceGone reports the app destroyed.
	InstanceGone(app string)
	// Restart counts one automatic restart.
	Restart(app string)
}

type noObserver struct{}

func (noObserver) Deploy(string, string) {}

func (noObserver) Stage(string, time.Duration) {}

func (noObserver) InstanceState(string, string) {}

func (noObserver) InstanceGone(string) {}

func (noObserver) Restart(string) {}

// Deploy stages, as reported to the Observer.
const (
	StageBuild  = "build"
	StageStart  = "start"
	StageHealth = "health"
	StageSwap   = "swap"
)

// Deploy results, as reported to the Observer.
const (
	ResultLive   = "live"
	ResultFailed = "failed"
)
