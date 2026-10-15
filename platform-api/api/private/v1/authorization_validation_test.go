package v1

import (
	"context"
	"strings"
	"testing"
)

func TestControlPlaneUpgradePolicyPermissionsAreValid(t *testing.T) {
	permissions := []string{
		"controlplaneupgradepolicy.create",
		"controlplaneupgradepolicy.list",
		"controlplaneupgradepolicy.get",
		"controlplaneupgradepolicy.update",
		"controlplaneupgradepolicy.delete",
	}

	for _, permission := range permissions {
		if !IsValidAuthorizationPermission(permission) {
			t.Errorf("permission %q is not valid", permission)
		}
	}
}

func TestRoleBindingConditionValidation(t *testing.T) {
	validCondition := `context.resourcePlural == "clusters" && context.spec.platform.gcp.region == "us-east1"`
	invalidSyntax := `context.spec.platform.gcp.region ==`
	namespaceReference := `resource in Namespace::"other"`
	stringLiteral := `context.resourceName == "Namespace::literal"`

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
	invalidSyntax := `context.spec ==`
	binding := testRoleBinding(&invalidSyntax)
	err := binding.ValidateUpdate(context.Background(), testRoleBinding(nil))
	if err == nil || !strings.Contains(err.Error(), "spec.condition is not a valid Cedar expression") {
		t.Fatalf("ValidateUpdate() error = %v, want invalid condition error", err)
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
