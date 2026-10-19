package native

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// nobody is the uid and gid an image without a USER runs as.
const nobody = 65534

// lookupUser resolves an image's User ("", "1000", "1000:1000", "app",
// "app:app") against the rootfs's /etc/passwd and /etc/group. An empty
// User means nobody; a numeric id needs no file.
func lookupUser(rootfs, user string) (uid, gid int, err error) {
	if user == "" {
		return nobody, nobody, nil
	}
	u, g, hasGroup := strings.Cut(user, ":")
	uid, gidFromPasswd, err := lookupID(filepath.Join(rootfs, "etc", "passwd"), u, true)
	if err != nil {
		return 0, 0, fmt.Errorf("image user %q: %w", user, err)
	}
	if !hasGroup {
		return uid, gidFromPasswd, nil
	}
	gid, _, err = lookupID(filepath.Join(rootfs, "etc", "group"), g, false)
	if err != nil {
		return 0, 0, fmt.Errorf("image group %q: %w", g, err)
	}
	return uid, gid, nil
}

// lookupID returns the id for name, numeric or looked up in a passwd- or
// group-style file (name:x:id:gid...). For passwd files the user's
// primary group is returned too.
func lookupID(file, name string, passwd bool) (id, gid int, err error) {
	if n, err := strconv.Atoi(name); err == nil {
		if n < 0 {
			return 0, 0, fmt.Errorf("%d is negative", n)
		}
		return n, nobody, nil
	}
	f, err := os.Open(file)
	if err != nil {
		return 0, 0, fmt.Errorf("%s not in the image and no %s to look it up", name, filepath.Base(file))
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Split(sc.Text(), ":")
		if len(fields) < 3 || fields[0] != name {
			continue
		}
		id, err := strconv.Atoi(fields[2])
		if err != nil {
			return 0, 0, fmt.Errorf("%s: bad id for %s", file, name)
		}
		if passwd && len(fields) >= 4 {
			gid, _ = strconv.Atoi(fields[3])
		}
		return id, gid, nil
	}
	return 0, 0, fmt.Errorf("%s not found in %s", name, filepath.Base(file))
}
