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

// channelIsNewer reports whether the desired channel tracks a higher operator
// version than the installed one.
//
// Only channels sharing a prefix and ending in a dotted version can be
// compared, so "stable-v1" to "stable-v1.4" is an upgrade while "latest" or a
// rename such as "stable" to "fast" is not. Anything that cannot be compared is
// reported as not newer: leaving an operator on its current channel is always
// safer than moving it to one that may turn out to be older.
func channelIsNewer(installed, desired string) bool {
	installedPrefix, installedVersion := parseChannel(installed)
	desiredPrefix, desiredVersion := parseChannel(desired)

	if installedVersion == nil || desiredVersion == nil || installedPrefix != desiredPrefix {
		return false
	}

	return compareVersions(desiredVersion, installedVersion) > 0
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
