package reconcilers

import (
	"cmp"
	"regexp"
	"strconv"
	"strings"
)

// channelVersionRE splits a channel name into its prefix and the dotted version
// that ends it, for example "stable-v1.4" into "stable-v" and "1.4". A channel
// that does not end in a version, such as "latest", does not match.
var channelVersionRE = regexp.MustCompile(`^(.*?)(\d+(?:\.\d+)*)$`)

// shouldMoveChannel reports whether the addon should move an operator from the
// channel it is installed on onto the desired one.
//
// The configured channel is an instruction and is applied, with one exception:
// the addon will not walk an operator backwards through a release series. That
// means a rollback is refused only when both channels carry a version under
// the same prefix and the desired version is the lower of the two, as in
// "stable-v1.5" to "stable-v1.4".
//
// Everything else moves. Versions under different prefixes, such as
// "stable-v1.4" to "fast-v1.5", number unrelated series and say nothing about
// which release is older, so there is no rollback to refuse. The same goes for
// a channel carrying no version at all, such as "latest" or a rename from
// "stable" to "fast".
func shouldMoveChannel(installed, desired string) bool {
	installedPrefix, installedVersion := parseChannel(installed)
	desiredPrefix, desiredVersion := parseChannel(desired)

	// Only two versions in the same series can be ordered against each other.
	if installedVersion == nil || desiredVersion == nil || installedPrefix != desiredPrefix {
		return true
	}

	return compareVersions(desiredVersion, installedVersion) >= 0
}

// parseChannel returns the channel's prefix and its trailing version. The
// version is nil for a channel that carries none.
func parseChannel(channel string) (string, []int) {
	match := channelVersionRE.FindStringSubmatch(strings.TrimSpace(channel))
	if match == nil {
		return channel, nil
	}

	segments := strings.Split(match[2], ".")
	version := make([]int, 0, len(segments))
	for _, segment := range segments {
		n, err := strconv.Atoi(segment)
		if err != nil {
			// Only reachable for a segment too large to hold in an int.
			return channel, nil
		}
		version = append(version, n)
	}

	return match[1], version
}

// compareVersions compares two versions segment by segment. A missing segment
// counts as zero, so 1.4 and 1.4.0 are equal.
func compareVersions(a, b []int) int {
	for i := 0; i < len(a) || i < len(b); i++ {
		var av, bv int
		if i < len(a) {
			av = a[i]
		}
		if i < len(b) {
			bv = b[i]
		}
		if c := cmp.Compare(av, bv); c != 0 {
			return c
		}
	}
	return 0
}
