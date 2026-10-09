package authz

import (
	"net/http"
	"testing"
)

func TestActionForControlPlaneUpgradeRequest(t *testing.T) {
	tests := []struct {
		name   string
		method string
		named  bool
		want   Action
	}{
		{name: "create", method: http.MethodPost, want: CreateControlPlaneUpgradeRequest},
		{name: "list", method: http.MethodGet, want: ListControlPlaneUpgradeRequests},
		{name: "get", method: http.MethodGet, named: true, want: GetControlPlaneUpgradeRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := actionForRequest(tt.method, "controlplaneupgraderequests", tt.named)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("action = %q, want %q", got, tt.want)
			}
		})
	}
}
