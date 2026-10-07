package hanwen

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Read the mount table without touching a possibly stalled FUSE filesystem.
func checkMountpoint(path string) error {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	defer f.Close()
	return checkMountinfo(f, path)
}

func checkMountinfo(f interface{ Read([]byte) (int, error) }, path string) error {
	scanner := bufio.NewScanner(f)
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			return fmt.Errorf("malformed mountinfo entry")
		}
		if unescape.Replace(fields[4]) == path {
			return fmt.Errorf("refusing to replace existing mount at %q; unmount it before starting DFS", path)
		}
	}
	return scanner.Err()
}
