package controlplaneupgrade

import "testing"

func TestSelectZStreamCandidate(t *testing.T) {
	tests := []struct {
		name       string
		completed  string
		advertised []string
		want       string
		wantErr    bool
	}{
		{
			name:       "newer patch in same minor is selected",
			completed:  "4.15.0",
			advertised: []string{"4.15.1"},
			want:       "4.15.1",
		},
		{
			name:       "newest of several patches is selected",
			completed:  "4.15.0",
			advertised: []string{"4.15.1", "4.15.3", "4.15.2"},
			want:       "4.15.3",
		},
		{
			name:       "cross-minor candidates are ignored",
			completed:  "4.15.0",
			advertised: []string{"4.16.0"},
			want:       "",
		},
		{
			name:       "candidates at or below completed are ignored",
			completed:  "4.15.2",
			advertised: []string{"4.15.0", "4.15.2"},
			want:       "",
		},
		{
			name:       "no advertised updates",
			completed:  "4.15.0",
			advertised: nil,
			want:       "",
		},
		{
			name:       "unparseable candidates are skipped, not fatal",
			completed:  "4.15.0",
			advertised: []string{"not-a-version", "4.15.1"},
			want:       "4.15.1",
		},
		{
			name:       "unparseable completed version is an error",
			completed:  "not-a-version",
			advertised: []string{"4.15.1"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectZStreamCandidate(tt.completed, tt.advertised)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("selectZStreamCandidate() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectYStreamCandidateRejectsDifferentFleetMajor(t *testing.T) {
	target, err := selectYStreamCandidate("4.22.14", []string{"4.23.0"}, "5.23")
	if err == nil {
		t.Fatal("expected a fleet major-version mismatch error")
	}
	if target != "" {
		t.Fatalf("selected target %q despite mismatched fleet major", target)
	}
}

func TestSelectYStreamCandidate(t *testing.T) {
	tests := []struct {
		name       string
		completed  string
		advertised []string
		fleetMinor string
		want       string
	}{
		{
			name: "next minor is selected at the fleet boundary", completed: "4.22.14",
			advertised: []string{"4.23.0"}, fleetMinor: "4.23", want: "4.23.0",
		},
		{
			name: "next minor is blocked below the fleet boundary", completed: "4.22.14",
			advertised: []string{"4.23.0"}, fleetMinor: "4.22",
		},
		{
			name: "no next minor advertised", completed: "4.22.14",
			advertised: []string{"4.22.17"}, fleetMinor: "4.23",
		},
		{
			name: "non-sequential minor is skipped", completed: "4.22.14",
			advertised: []string{"4.24.0"}, fleetMinor: "4.24",
		},
		{
			name: "newest patch in next minor is selected", completed: "4.22.14",
			advertised: []string{"4.23.1", "4.23.3", "4.23.2"}, fleetMinor: "4.23", want: "4.23.3",
		},
		{
			name: "invalid advertised version is skipped", completed: "4.22.14",
			advertised: []string{"bad-version", "4.23.0-ec.1"}, fleetMinor: "4.23", want: "4.23.0-ec.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectYStreamCandidate(tt.completed, tt.advertised, tt.fleetMinor)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("selectYStreamCandidate() = %q, want %q", got, tt.want)
			}
		})
	}
}
