package native

import (
	"archive/tar"
	"fmt"

	"golang.org/x/sys/unix"
)

// mknod creates the device or fifo node a tar entry describes. Root only.
func mknod(target string, h *tar.Header) error {
	var kind uint32
	switch h.Typeflag {
	case tar.TypeChar:
		kind = unix.S_IFCHR
	case tar.TypeBlock:
		kind = unix.S_IFBLK
	case tar.TypeFifo:
		kind = unix.S_IFIFO
	default:
		return fmt.Errorf("%s: not a device entry", h.Name)
	}
	dev := unix.Mkdev(uint32(h.Devmajor), uint32(h.Devminor)) //nolint:gosec // major and minor numbers are small
	mode := kind | uint32(h.FileInfo().Mode().Perm())
	if err := unix.Mknod(target, mode, int(dev)); err != nil {
		return fmt.Errorf("mknod %s: %w", target, err)
	}
	return nil
}
