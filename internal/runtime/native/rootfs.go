// Package native runs instances without Docker at runtime: an image's
// filesystem is exported once, and each instance is the daemon binary
// re-executed into new namespaces, where it mounts an overlay over that
// filesystem, pivots into it, drops its privileges, and execs the app.
// Docker still builds the image; this package only needs it to export one.
package native

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Docker is the slice of the Engine API the export needs. *client.Client
// satisfies it; tests substitute a fake.
type Docker interface {
	ImageInspect(ctx context.Context, imageID string, opts ...client.ImageInspectOption) (client.ImageInspectResult, error)
	ContainerCreate(ctx context.Context, options client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerExport(ctx context.Context, containerID string, options client.ContainerExportOptions) (client.ContainerExportResult, error)
	ContainerRemove(ctx context.Context, containerID string, options client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
}

// Image is an exported image: where its filesystem is and how the image
// says it should be run.
type Image struct {
	ID     string // the content-addressable id, without the "sha256:" prefix
	Dir    string // <dataDir>/rootfs/<ID>
	Config ImageConfig
}

// Rootfs is the directory holding the image's files, the overlay's lower
// layer.
func (im Image) Rootfs() string { return filepath.Join(im.Dir, "rootfs") }

// ImageConfig is the part of an OCI image config that decides what runs.
type ImageConfig struct {
	Entrypoint []string `json:"entrypoint,omitempty"`
	Cmd        []string `json:"cmd,omitempty"`
	Env        []string `json:"env,omitempty"`
	WorkingDir string   `json:"working_dir,omitempty"`
	User       string   `json:"user,omitempty"`
}

// Argv is the command the image runs: Entrypoint followed by Cmd, as Docker
// combines them.
func (c ImageConfig) Argv() []string {
	return append(append([]string{}, c.Entrypoint...), c.Cmd...)
}

const (
	configFile = "config.json"
	doneFile   = "done" // written last; its presence means the export is complete
)

// exporter turns images into rootfs directories once each.
type exporter struct {
	docker Docker
	dir    string // <dataDir>/rootfs

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per image id, so two deploys of one image export once
}

func newExporter(d Docker, dataDir string) *exporter {
	return &exporter{docker: d, dir: filepath.Join(dataDir, "rootfs"), locks: make(map[string]*sync.Mutex)}
}

// Prepare makes sure the image's filesystem is on disk and returns it. The
// export runs once per image id; a second call, or a call for an image
// already exported by an earlier daemon, returns at once. An export that
// was cut off (no done marker) is redone from scratch.
func (e *exporter) Prepare(ctx context.Context, image string) (Image, error) {
	info, err := e.docker.ImageInspect(ctx, image)
	if err != nil {
		return Image{}, fmt.Errorf("inspect %s: %w", image, err)
	}
	id := strings.TrimPrefix(info.ID, "sha256:")
	if id == "" || strings.ContainsAny(id, "/\\") {
		return Image{}, fmt.Errorf("inspect %s: bad image id %q", image, info.ID)
	}
	im := Image{ID: id, Dir: filepath.Join(e.dir, id)}
	if info.Config != nil {
		im.Config = ImageConfig{Entrypoint: info.Config.Entrypoint, Cmd: info.Config.Cmd, Env: info.Config.Env,
			WorkingDir: info.Config.WorkingDir, User: info.Config.User}
	}

	e.mu.Lock()
	lock, ok := e.locks[id]
	if !ok {
		lock = &sync.Mutex{}
		e.locks[id] = lock
	}
	e.mu.Unlock()
	lock.Lock()
	defer lock.Unlock()

	if _, err := os.Stat(filepath.Join(im.Dir, doneFile)); err == nil {
		cfg, err := readConfig(im.Dir)
		if err != nil {
			return Image{}, err
		}
		im.Config = cfg // what was exported, should the image have been rebuilt under the same tag
		return im, nil
	}
	if err := e.export(ctx, image, im); err != nil {
		_ = os.RemoveAll(im.Dir)
		return Image{}, err
	}
	return im, nil
}

// export creates a container from the image (never started), streams its
// filesystem as a tar, unpacks it, writes the config, then the marker.
func (e *exporter) export(ctx context.Context, image string, im Image) error {
	if err := os.RemoveAll(im.Dir); err != nil {
		return err
	}
	if err := os.MkdirAll(im.Rootfs(), 0o755); err != nil {
		return err
	}
	res, err := e.docker.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:     &container.Config{Image: image, Labels: map[string]string{"ashore.export": "1"}},
		HostConfig: &container.HostConfig{},
	})
	if err != nil {
		return fmt.Errorf("create export container for %s: %w", image, err)
	}
	defer func() {
		_, _ = e.docker.ContainerRemove(context.WithoutCancel(ctx), res.ID, client.ContainerRemoveOptions{Force: true})
	}()
	rc, err := e.docker.ContainerExport(ctx, res.ID, client.ContainerExportOptions{})
	if err != nil {
		return fmt.Errorf("export %s: %w", image, err)
	}
	defer func() { _ = rc.Close() }()
	if err := untar(rc, im.Rootfs()); err != nil {
		return fmt.Errorf("unpack %s: %w", image, err)
	}
	cfg, err := json.MarshalIndent(im.Config, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(im.Dir, configFile), cfg, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(im.Dir, doneFile), nil, 0o644)
}

func readConfig(dir string) (ImageConfig, error) {
	data, err := os.ReadFile(filepath.Join(dir, configFile))
	if err != nil {
		return ImageConfig{}, err
	}
	var cfg ImageConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return ImageConfig{}, fmt.Errorf("%s: %w", filepath.Join(dir, configFile), err)
	}
	return cfg, nil
}

// untar unpacks a container export into dir. Every entry's path is
// confined to dir, so an image cannot write outside its rootfs; modes are
// kept; ownership and device nodes need root and are skipped without it
// (a rootless runtime runs everything as one user anyway). Hard links are
// made as links when the target was already unpacked, otherwise copied
// later would be wrong, so the export's order (targets first) is relied on.
func untar(r io.Reader, dir string) error {
	tr := tar.NewReader(r)
	root := os.Geteuid() == 0
	var deferredDirs []*tar.Header // directories whose mode is set last, so files can be written into read-only ones
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		target, err := secureJoin(dir, h.Name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		mode := h.FileInfo().Mode()
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			deferredDirs = append(deferredDirs, h)
		case tar.TypeReg:
			if err := writeFile(target, tr, mode.Perm()); err != nil {
				return err
			}
		case tar.TypeSymlink:
			_ = os.Remove(target)
			if err := os.Symlink(h.Linkname, target); err != nil {
				return err
			}
		case tar.TypeLink:
			src, err := secureJoin(dir, h.Linkname)
			if err != nil {
				return err
			}
			_ = os.Remove(target)
			if err := os.Link(src, target); err != nil {
				return err
			}
		case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
			if !root {
				continue
			}
			if err := mknod(target, h); err != nil {
				return err
			}
		default:
			continue // extended headers and the like
		}
		if root && h.Typeflag != tar.TypeSymlink && h.Typeflag != tar.TypeDir {
			if err := os.Lchown(target, h.Uid, h.Gid); err != nil {
				return err
			}
		}
	}
	for i := len(deferredDirs) - 1; i >= 0; i-- {
		h := deferredDirs[i]
		target, _ := secureJoin(dir, h.Name)
		if root {
			if err := os.Chown(target, h.Uid, h.Gid); err != nil {
				return err
			}
		}
		perm := h.FileInfo().Mode().Perm()
		if !root {
			perm |= 0o700 // the exporting user must be able to enter and write it
		}
		if err := os.Chmod(target, perm); err != nil {
			return err
		}
	}
	return nil
}

func writeFile(target string, r io.Reader, perm fs.FileMode) error {
	_ = os.Remove(target) // a stale symlink would otherwise be followed
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// OpenFile applies the umask; the image's mode is what the app expects.
	return os.Chmod(target, perm)
}

// secureJoin resolves name inside dir and refuses anything that would
// land outside it.
func secureJoin(dir, name string) (string, error) {
	clean := filepath.Clean("/" + name)
	target := filepath.Join(dir, clean)
	if target != dir && !strings.HasPrefix(target, dir+string(filepath.Separator)) {
		return "", fmt.Errorf("tar entry %q escapes the rootfs", name)
	}
	return target, nil
}
