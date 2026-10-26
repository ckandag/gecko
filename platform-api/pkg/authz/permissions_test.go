package authz

import (
	"net/http"
	"testing"
)

func TestActionForControlPlaneUpgradePolicyRequest(t *testing.T) {
	tests := []struct {
		name   string
		method string
		named  bool
		want   Action
	}{
		{name: "create", method: http.MethodPost, want: CreateControlPlaneUpgradePolicy},
		{name: "list", method: http.MethodGet, want: ListControlPlaneUpgradePolicies},
		{name: "get", method: http.MethodGet, named: true, want: GetControlPlaneUpgradePolicy},
		{name: "update", method: http.MethodPut, named: true, want: UpdateControlPlaneUpgradePolicy},
		{name: "delete", method: http.MethodDelete, named: true, want: DeleteControlPlaneUpgradePolicy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := actionForRequest(tt.method, "controlplaneupgradepolicies", tt.named)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("action = %q, want %q", got, tt.want)
			}
		})
	}
}
