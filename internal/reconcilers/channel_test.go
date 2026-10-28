package reconcilers

import "testing"

func TestChannelIsNewer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		installed string
		desired   string
		want      bool
	}{
		{"minor bump", "stable-v1.4", "stable-v1.5", true},
		{"major bump", "stable-v1.4", "stable-v2.0", true},
		{"version added to bare major", "stable-v1", "stable-v1.4", true},
		{"two digit minor beats one digit", "stable-v1.9", "stable-v1.10", true},
		{"same channel", "stable-v1.4", "stable-v1.4", false},
		{"trailing zero is the same version", "stable-v1.4", "stable-v1.4.0", false},
		{"downgrade", "stable-v1.5", "stable-v1.4", false},
		{"downgrade to bare major", "stable-v1.4", "stable-v1", false},
		{"unversioned on both sides", "latest", "latest", false},
		// A rename is not an upgrade: the addon cannot tell which of two
		// differently named channels is ahead, so it leaves the operator put.
		{"different prefix", "stable-v1.4", "fast-v1.5", false},
		{"unversioned installed", "latest", "stable-v1.4", false},
		{"unversioned desired", "stable-v1.4", "latest", false},
		{"empty installed", "", "stable-v1.4", false},
		{"empty desired", "stable-v1.4", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := channelIsNewer(tc.installed, tc.desired); got != tc.want {
				t.Errorf("channelIsNewer(%q, %q) = %v, want %v", tc.installed, tc.desired, got, tc.want)
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
