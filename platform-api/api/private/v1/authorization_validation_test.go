package v1

import (
	"context"
	"strings"
	"testing"
)

func TestRoleBindingConditionValidation(t *testing.T) {
	validCondition := `context.resourcePlural == "clusters" && context.spec.platform.gcp.region == "us-east1"`
	invalidSyntax := `context.spec.platform.gcp.region ==`
	namespaceReference := `resource in Namespace::"other"`
	stringLiteral := `context.resourceName == "Namespace::literal"`
	trailingPolicy := `true }; forbid (principal, action, resource) when { true`

	tests := []struct {
		name      string
		condition *string
		wantErr   string
	}{
		{name: "omitted", condition: nil},
		{name: "valid expression", condition: &validCondition},
		{name: "string literal is not an entity reference", condition: &stringLiteral},
		{name: "invalid syntax", condition: &invalidSyntax, wantErr: "spec.condition is not a valid Cedar expression"},
		{name: "namespace entity", condition: &namespaceReference, wantErr: "must not reference Namespace:: entities"},
		{name: "trailing policy", condition: &trailingPolicy, wantErr: "must be a single Cedar condition expression; trailing Cedar policy statements are not allowed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := testRoleBinding(tt.condition)
			err := binding.ValidateCreate(context.Background())
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateCreate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateCreate() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRoleBindingConditionValidationOnUpdate(t *testing.T) {
	for _, condition := range []struct {
		name    string
		value   string
		wantErr string
	}{
		{
			name:    "invalid syntax",
			value:   `context.spec ==`,
			wantErr: "spec.condition is not a valid Cedar expression",
		},
		{
			name:    "trailing policy",
			value:   `true }; forbid (principal, action, resource) when { true`,
			wantErr: "must be a single Cedar condition expression; trailing Cedar policy statements are not allowed",
		},
	} {
		t.Run(condition.name, func(t *testing.T) {
			binding := testRoleBinding(&condition.value)
			err := binding.ValidateUpdate(context.Background(), testRoleBinding(nil))
			if err == nil || !strings.Contains(err.Error(), condition.wantErr) {
				t.Fatalf("ValidateUpdate() error = %v, want containing %q", err, condition.wantErr)
			}
		})
	}
}

func testRoleBinding(condition *string) *RoleBinding {
	return &RoleBinding{
		Spec: RoleBindingSpec{
			Subject:   "alice@example.com",
			RoleRef:   RoleRef{Kind: "PlatformRole", Name: "viewer", APIGroup: GroupVersion.Group},
			Condition: condition,
		},
	}
}
