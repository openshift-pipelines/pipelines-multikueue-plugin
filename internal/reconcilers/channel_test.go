package reconcilers

import "testing"

func TestShouldMoveChannel(t *testing.T) {
	for _, tc := range []struct {
		name      string
		installed string
		desired   string
		want      bool
	}{
		// A rollback within one release series is the only move refused.
		{"downgrade", "stable-v1.5", "stable-v1.4", false},
		{"downgrade to bare major", "stable-v1.4", "stable-v1", false},

		{"minor bump", "stable-v1.4", "stable-v1.5", true},
		{"major bump", "stable-v1.4", "stable-v2.0", true},
		{"version added to bare major", "stable-v1", "stable-v1.4", true},
		{"two digit minor beats one digit", "stable-v1.9", "stable-v1.10", true},
		// Equal versions spelled differently are not a rollback, so the
		// configured spelling is applied. upgradeSubscription returns before
		// calling this when the two channels are byte-for-byte equal.
		{"trailing zero is the same version", "stable-v1.4", "stable-v1.4.0", true},

		// Versions under different prefixes number unrelated series, so
		// neither is older than the other and the configured channel wins —
		// in both directions.
		{"different prefix, higher version", "stable-v1.4", "fast-v1.5", true},
		{"different prefix, lower version", "stable-v1.5", "fast-v1.4", true},

		// A channel carrying no version cannot be behind anything, whichever
		// side of the move it is on.
		{"unversioned installed", "latest", "stable-v1.4", true},
		{"unversioned installed, matching prefix", "stable", "stable-v1.4", true},
		{"unversioned desired", "stable-v1.4", "latest", true},
		{"unversioned on both sides", "latest", "stable", true},
		{"unversioned rename", "stable", "fast", true},
		{"empty installed", "", "stable-v1.4", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldMoveChannel(tc.installed, tc.desired); got != tc.want {
				t.Errorf("shouldMoveChannel(%q, %q) = %v, want %v", tc.installed, tc.desired, got, tc.want)
			}
		})
	}
}

func TestParseChannel(t *testing.T) {
	for _, tc := range []struct {
		channel     string
		wantPrefix  string
		wantVersion []int
	}{
		{"stable-v1.4", "stable-v", []int{1, 4}},
		{"stable-v1", "stable-v", []int{1}},
		{"pipelines-1.19", "pipelines-", []int{1, 19}},
		{"1.4", "", []int{1, 4}},
		{"latest", "latest", nil},
		{"stable", "stable", nil},
		{"", "", nil},
	} {
		t.Run(tc.channel, func(t *testing.T) {
			prefix, version := parseChannel(tc.channel)
			if prefix != tc.wantPrefix {
				t.Errorf("prefix = %q, want %q", prefix, tc.wantPrefix)
			}
			if len(version) != len(tc.wantVersion) {
				t.Fatalf("version = %v, want %v", version, tc.wantVersion)
			}
			for i := range version {
				if version[i] != tc.wantVersion[i] {
					t.Fatalf("version = %v, want %v", version, tc.wantVersion)
				}
			}
		})
	}
}
