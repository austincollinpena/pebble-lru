package lruCache

import (
	"os"
	"path/filepath"
)

// https://stackoverflow.com/a/33451503/12563520
// https://creativecommons.org/licenses/by-sa/3.0/
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}
