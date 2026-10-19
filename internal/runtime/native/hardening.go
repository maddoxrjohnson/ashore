package native

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// harden is what the child does to itself after the pivot and before the
// exec, so the app starts with as little as possible: no capabilities in
// any set, no way to gain privileges through setuid binaries, and the
// image's user instead of root. The order matters: the bounding set and
// the uid switch need capabilities the step before still has.
//
// Rootless instances skip the uid switch: in their user namespace only the
// daemon's own uid is mapped, so there is no other identity to take, and
// uid 0 inside is the unprivileged user outside anyway. They still lose
// every capability, so "root" in the container can do nothing root can.
func harden(spec initSpec) error {
	last, err := lastCap()
	if err != nil {
		return err
	}
	// The bounding set caps what any exec, now or later, can hand out.
	for c := 0; c <= last; c++ {
		if err := unix.Prctl(unix.PR_CAPBSET_DROP, uintptr(c), 0, 0, 0); err != nil { //nolint:gosec // c is a small capability number
			return fmt.Errorf("drop capability %d from the bounding set: %w", c, err)
		}
	}
	if err := unix.Prctl(unix.PR_CAP_AMBIENT, unix.PR_CAP_AMBIENT_CLEAR_ALL, 0, 0, 0); err != nil {
		return fmt.Errorf("clear ambient capabilities: %w", err)
	}
	if !spec.Rootless {
		if err := unix.Setgroups([]int{spec.GID}); err != nil {
			return fmt.Errorf("setgroups: %w", err)
		}
		if err := unix.Setresgid(spec.GID, spec.GID, spec.GID); err != nil {
			return fmt.Errorf("setgid %d: %w", spec.GID, err)
		}
		if err := unix.Setresuid(spec.UID, spec.UID, spec.UID); err != nil {
			return fmt.Errorf("setuid %d: %w", spec.UID, err)
		}
	}
	// Leaving root clears the permitted and effective sets by itself; an
	// image that runs as root, or a rootless instance, needs it done by
	// hand, and doing it always is simpler than knowing.
	hdr := unix.CapUserHeader{Version: unix.LINUX_CAPABILITY_VERSION_3}
	var none [2]unix.CapUserData
	if err := unix.Capset(&hdr, &none[0]); err != nil {
		return fmt.Errorf("drop capabilities: %w", err)
	}
	// From here on no exec can grant more than the process has, which is
	// nothing: setuid bits and file capabilities in the image are inert.
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("set no_new_privs: %w", err)
	}
	return nil
}

// lastCap is the highest capability number this kernel knows, so the
// bounding-set loop covers capabilities newer than the constants compiled
// in.
func lastCap() (int, error) {
	data, err := os.ReadFile("/proc/sys/kernel/cap_last_cap")
	if err != nil {
		return unix.CAP_LAST_CAP, nil //nolint:nilerr // an old /proc; the compiled-in value is the next best thing
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("cap_last_cap: %w", err)
	}
	return n, nil
}
