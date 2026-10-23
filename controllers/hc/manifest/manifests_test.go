package manifest_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openshift-online/gecko/controllers/hc/manifest"
)

func testInput() manifest.Input {
	return manifest.Input{
		ClusterID:                    "550e8400-e29b-41d4-a716-446655440000",
		ClusterName:                  "my-cluster-safe",
		Generation:                   3,
		CreatedBy:                    "alice@redhat.com",
		InfraID:                      "infra-xyz",
		IssuerURL:                    "https://issuer.example.com",
		GCPProjectID:                 "my-gcp-project",
		GCPRegion:                    "us-central1",
		GCPNetwork:                   "my-vpc",
		GCPSubnet:                    "my-subnet",
		GCPEndpointAccess:            "Private",
		WIFProjectNumber:             "123456789",
		WIFPoolID:                    "my-pool",
		WIFProviderID:                "my-provider",
		NodePoolEmail:                "nodepool@project.iam.gserviceaccount.com",
		ControlPlaneEmail:            "cp@project.iam.gserviceaccount.com",
		CloudControllerEmail:         "cc@project.iam.gserviceaccount.com",
		StorageEmail:                 "storage@project.iam.gserviceaccount.com",
		ImageRegistryEmail:           "registry@project.iam.gserviceaccount.com",
		NetworkEmail:                 "network@project.iam.gserviceaccount.com",
		ReleaseImage:                 "quay.io/openshift-release-dev/ocp-release:4.15.0-x86_64",
		ReleaseChannel:               "stable-4.15",
		BaseDomain:                   "example.com",
		PullSecretStoreName:          "gcp-secret-manager",
		PullSecretGCPKey:             "default-openshift-pull-secret",
		ControllerAvailabilityPolicy: "HighlyAvailable",
		CPOImage:                     "",
		CAPGImage:                    "",
		Slug:                         "alice",
	}
}

func TestBuild_UsesClusterIDForNamespaceAndHostedClusterSpec(t *testing.T) {
	input := testInput()
	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var ns map[string]any
	require.NoError(t, json.Unmarshal(manifests[0], &ns))
	nsMeta := ns["metadata"].(map[string]any)
	require.Equal(t, "clusters-550e8400-e29b-41d4-a716-446655440000", nsMeta["name"])

	var hc map[string]any
	require.NoError(t, json.Unmarshal(manifests[3], &hc))
	hcMeta := hc["metadata"].(map[string]any)
	require.Equal(t, "my-cluster-safe", hcMeta["name"])
	require.Equal(t, "clusters-550e8400-e29b-41d4-a716-446655440000", hcMeta["namespace"])
	require.LessOrEqual(t, len(hcMeta["namespace"].(string)+"-"+hcMeta["name"].(string)), 63)

	labels := hcMeta["labels"].(map[string]any)
	require.Equal(t, input.ClusterID, labels["gcp.managed.openshift.io/cluster-id"])

	spec := hc["spec"].(map[string]any)
	require.Equal(t, input.ClusterID, spec["clusterID"])
}

func TestBuild_HostedControlPlaneNamespaceWithinLimit(t *testing.T) {
	input := testInput()
	input.ClusterName = "seventeen-chars-o"
	require.Len(t, input.ClusterName, 17)

	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var job map[string]any
	require.NoError(t, json.Unmarshal(manifests[4], &job))
	metadata := job["metadata"].(map[string]any)
	require.Equal(t, "clusters-550e8400-e29b-41d4-a716-446655440000-seventeen-chars-o", metadata["namespace"])
	require.Len(t, metadata["namespace"], 63)
}

func TestBuild_ReturnsManifests(t *testing.T) {
	input := testInput()
	manifests, err := manifest.Build(input)
	require.NoError(t, err)
	require.NotNil(t, manifests)
}

func TestBuild_FiveManifests(t *testing.T) {
	input := testInput()
	manifests, err := manifest.Build(input)
	require.NoError(t, err)
	require.Len(t, manifests, 5, "expected 5 manifests: Namespace, ExternalSecret, Certificate, HostedCluster, Job")
}

func TestBuild_ManifestKinds(t *testing.T) {
	input := testInput()
	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	expectedKinds := []string{"Namespace", "ExternalSecret", "Certificate", "HostedCluster", "Job"}
	for i, m := range manifests {
		var obj map[string]any
		require.NoError(t, json.Unmarshal(m, &obj))
		kind, ok := obj["kind"].(string)
		require.True(t, ok, "manifest[%d] missing kind", i)
		require.Equal(t, expectedKinds[i], kind, "manifest[%d] wrong kind", i)
	}
}

func TestBuild_HostedClusterReleaseImage(t *testing.T) {
	input := testInput()
	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	// HostedCluster is at index 3.
	var obj map[string]any
	require.NoError(t, json.Unmarshal(manifests[3], &obj))

	spec, ok := obj["spec"].(map[string]any)
	require.True(t, ok, "HostedCluster missing spec")
	release, ok := spec["release"].(map[string]any)
	require.True(t, ok, "HostedCluster spec missing release")
	require.Equal(t, input.ReleaseImage, release["image"])
}

func TestBuild_JobName(t *testing.T) {
	input := testInput()
	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(manifests[4], &obj))

	meta, ok := obj["metadata"].(map[string]any)
	require.True(t, ok, "Job missing metadata")
	expectedJobName := fmt.Sprintf("rbac-setup-gen-%d", input.Generation)
	require.Equal(t, expectedJobName, meta["name"])
}

func TestBuild_CPOAnnotation(t *testing.T) {
	input := testInput()
	input.CPOImage = "quay.io/openshift/hypershift:latest"

	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(manifests[3], &obj))

	meta := obj["metadata"].(map[string]any)
	annotations := meta["annotations"].(map[string]any)
	require.Equal(t, input.CPOImage, annotations["hypershift.openshift.io/control-plane-operator-image"])
}

func TestBuild_NoCPOAnnotationWhenEmpty(t *testing.T) {
	input := testInput()
	input.CPOImage = "" // empty — should not be set

	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(manifests[3], &obj))

	meta := obj["metadata"].(map[string]any)
	annotations := meta["annotations"].(map[string]any)
	_, hasCPO := annotations["hypershift.openshift.io/control-plane-operator-image"]
	require.False(t, hasCPO, "CPO annotation should not be set when CPOImage is empty")
}

func TestBuild_CleanupCloudResourcesAnnotation(t *testing.T) {
	input := testInput()

	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(manifests[3], &obj))

	meta := obj["metadata"].(map[string]any)
	annotations := meta["annotations"].(map[string]any)
	require.Equal(t, "true", annotations["hypershift.openshift.io/cleanup-cloud-resources"],
		"HostedCluster must opt in to cloud resource cleanup on deletion")
}

func TestBuild_DefaultEndpointAccess(t *testing.T) {
	input := testInput()
	input.GCPEndpointAccess = "" // should default to "Private"

	manifests, err := manifest.Build(input)
	require.NoError(t, err)

	var obj map[string]any
	require.NoError(t, json.Unmarshal(manifests[3], &obj))

	spec := obj["spec"].(map[string]any)
	platform := spec["platform"].(map[string]any)
	gcp := platform["gcp"].(map[string]any)
	require.Equal(t, "Private", gcp["endpointAccess"])
}

func TestBuild_RequiredFieldValidation(t *testing.T) {
	input := testInput()
	input.ClusterID = ""
	_, err := manifest.Build(input)
	require.Error(t, err)
}

func TestBuild_GenerationValidation(t *testing.T) {
	input := testInput()
	input.Generation = 0
	_, err := manifest.Build(input)
	require.Error(t, err)
}

func TestBuild_ResourceLabels(t *testing.T) {
	t.Run("single label: resourceLabels is a list with one key-value entry", func(t *testing.T) {
		input := testInput()
		input.ResourceLabels = map[string]string{"goog-partner-solution": "test-partner-solution"}

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[3], &obj))

		spec := obj["spec"].(map[string]any)
		platform := spec["platform"].(map[string]any)
		gcp := platform["gcp"].(map[string]any)

		labels, ok := gcp["resourceLabels"].([]any)
		require.True(t, ok, "resourceLabels should be a list")
		require.Len(t, labels, 1)

		entry := labels[0].(map[string]any)
		require.Equal(t, "goog-partner-solution", entry["key"])
		require.Equal(t, "test-partner-solution", entry["value"])
	})

	t.Run("multiple labels: resourceLabels contains all entries", func(t *testing.T) {
		input := testInput()
		input.ResourceLabels = map[string]string{
			"goog-partner-solution": "test-partner-solution",
			"env":                   "prod",
		}

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[3], &obj))

		spec := obj["spec"].(map[string]any)
		platform := spec["platform"].(map[string]any)
		gcp := platform["gcp"].(map[string]any)

		labels, ok := gcp["resourceLabels"].([]any)
		require.True(t, ok, "resourceLabels should be a list")
		require.Len(t, labels, 2)

		byKey := map[string]string{}
		for _, l := range labels {
			e := l.(map[string]any)
			byKey[e["key"].(string)] = e["value"].(string)
		}
		require.Equal(t, "test-partner-solution", byKey["goog-partner-solution"])
		require.Equal(t, "prod", byKey["env"])
	})

	t.Run("absent: resourceLabels not set when ResourceLabels is nil", func(t *testing.T) {
		input := testInput()
		input.ResourceLabels = nil

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[3], &obj))

		spec := obj["spec"].(map[string]any)
		platform := spec["platform"].(map[string]any)
		gcp := platform["gcp"].(map[string]any)

		_, hasLabels := gcp["resourceLabels"]
		require.False(t, hasLabels, "resourceLabels should not be present when ResourceLabels is nil")
	})
}

func TestBuild_PullSecretGCPKey(t *testing.T) {
	t.Run("custom key appears in ExternalSecret remoteRef", func(t *testing.T) {
		input := testInput()
		input.PullSecretGCPKey = "default-openshift-pull-secret-ci"

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		// ExternalSecret is at index 1.
		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[1], &obj))
		require.Equal(t, "ExternalSecret", obj["kind"])

		spec := obj["spec"].(map[string]any)
		data := spec["data"].([]any)
		require.Len(t, data, 1)
		entry := data[0].(map[string]any)
		remoteRef := entry["remoteRef"].(map[string]any)
		require.Equal(t, "default-openshift-pull-secret-ci", remoteRef["key"])
	})

	t.Run("default key used when empty", func(t *testing.T) {
		input := testInput()
		input.PullSecretGCPKey = ""

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[1], &obj))

		spec := obj["spec"].(map[string]any)
		data := spec["data"].([]any)
		entry := data[0].(map[string]any)
		remoteRef := entry["remoteRef"].(map[string]any)
		require.Equal(t, "default-openshift-pull-secret", remoteRef["key"])
	})
}

func TestBuild_PullSecretStoreName(t *testing.T) {
	t.Run("custom store name appears in ExternalSecret secretStoreRef", func(t *testing.T) {
		input := testInput()
		input.PullSecretStoreName = "custom-store"

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[1], &obj))
		require.Equal(t, "ExternalSecret", obj["kind"])

		spec := obj["spec"].(map[string]any)
		storeRef := spec["secretStoreRef"].(map[string]any)
		require.Equal(t, "custom-store", storeRef["name"])
	})

	t.Run("default store name used when empty", func(t *testing.T) {
		input := testInput()
		input.PullSecretStoreName = ""

		manifests, err := manifest.Build(input)
		require.NoError(t, err)

		var obj map[string]any
		require.NoError(t, json.Unmarshal(manifests[1], &obj))

		spec := obj["spec"].(map[string]any)
		storeRef := spec["secretStoreRef"].(map[string]any)
		require.Equal(t, "gcp-secret-manager", storeRef["name"])
	})
}
