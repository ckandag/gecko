// Package manifest provides the manifest builder for the hc-controller.
package manifest

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// Input holds all parameters needed to build the HC manifests.
type Input struct {
	ClusterID                    string
	ClusterName                  string // bounded internal HostedCluster name
	Generation                   int64
	CreatedBy                    string
	InfraID                      string
	IssuerURL                    string
	GCPProjectID                 string
	GCPRegion                    string
	GCPNetwork                   string
	GCPSubnet                    string
	GCPEndpointAccess            string // default: "Private"
	WIFProjectNumber             string
	WIFPoolID                    string
	WIFProviderID                string
	NodePoolEmail                string
	ControlPlaneEmail            string
	CloudControllerEmail         string
	StorageEmail                 string
	ImageRegistryEmail           string
	NetworkEmail                 string
	ReleaseImage                 string
	ReleaseChannel               string
	BaseDomain                   string
	PullSecretStoreName          string            // default: "gcp-secret-manager"
	PullSecretGCPKey             string            // default: "default-openshift-pull-secret"
	ControllerAvailabilityPolicy string            // default: "HighlyAvailable"
	CPOImage                     string            // optional — set CPO annotation if non-empty
	CAPGImage                    string            // optional — set CAPG annotation if non-empty
	Slug                         string            // default: "user" (username slug for DNS names)
	ResourceLabels               map[string]string // optional — sets spec.platform.gcp.resourceLabels on the HostedCluster CR
}

// Build constructs raw JSON bytes for each manifest resource from the given input.
// Returns an error if required fields are missing.
func Build(input Input) ([][]byte, error) {
	if err := validate(input); err != nil {
		return nil, err
	}

	// Apply defaults.
	if input.GCPEndpointAccess == "" {
		input.GCPEndpointAccess = "Private"
	}
	if input.PullSecretStoreName == "" {
		input.PullSecretStoreName = "gcp-secret-manager"
	}
	if input.PullSecretGCPKey == "" {
		input.PullSecretGCPKey = "default-openshift-pull-secret"
	}
	if input.ControllerAvailabilityPolicy == "" {
		input.ControllerAvailabilityPolicy = "HighlyAvailable"
	}
	if input.Slug == "" {
		input.Slug = "user"
	}

	clusterNS := fmt.Sprintf("clusters-%s", input.ClusterID)
	hcNS := fmt.Sprintf("clusters-%s-%s", input.ClusterID, input.ClusterName)

	manifests, err := buildManifests(input, clusterNS, hcNS)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: build manifests: %w", err)
	}

	return manifests, nil
}

func validate(input Input) error {
	required := map[string]string{
		"ClusterID":    input.ClusterID,
		"ClusterName":  input.ClusterName,
		"ReleaseImage": input.ReleaseImage,
		"BaseDomain":   input.BaseDomain,
		"GCPProjectID": input.GCPProjectID,
		"GCPRegion":    input.GCPRegion,
	}
	for field, val := range required {
		if val == "" {
			return fmt.Errorf("hc manifest: required field %s is empty", field)
		}
	}
	if input.Generation <= 0 {
		return fmt.Errorf("hc manifest: Generation must be > 0")
	}
	return nil
}

func buildManifests(input Input, clusterNS, hcNS string) ([][]byte, error) {
	ns, err := buildNamespace(input, clusterNS)
	if err != nil {
		return nil, err
	}
	es, err := buildExternalSecret(input, clusterNS)
	if err != nil {
		return nil, err
	}
	cert, err := buildCertificate(input, clusterNS)
	if err != nil {
		return nil, err
	}
	hc, err := buildHostedCluster(input, clusterNS)
	if err != nil {
		return nil, err
	}
	job, err := buildRBACJob(input, hcNS)
	if err != nil {
		return nil, err
	}

	return [][]byte{ns, es, cert, hc, job}, nil
}

func buildNamespace(input Input, clusterNS string) ([]byte, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	obj := map[string]any{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]any{
			"name": clusterNS,
			"labels": map[string]any{
				"gcp.managed.openshift.io/cluster-id":    input.ClusterID,
				"gcp.managed.openshift.io/cluster-name":  input.ClusterName,
				"gcp.managed.openshift.io/managed-by":    "hc-controller",
				"gcp.managed.openshift.io/resource-type": "namespace",
			},
			"annotations": map[string]any{
				"gcp.managed.openshift.io/generation": genStr,
			},
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: marshal namespace: %w", err)
	}
	return raw, nil
}

func buildExternalSecret(input Input, clusterNS string) ([]byte, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	obj := map[string]any{
		"apiVersion": "external-secrets.io/v1",
		"kind":       "ExternalSecret",
		"metadata": map[string]any{
			"name":      "pull-secret",
			"namespace": clusterNS,
			"annotations": map[string]any{
				"gcp.managed.openshift.io/generation": genStr,
			},
		},
		"spec": map[string]any{
			"refreshInterval": "1h",
			"secretStoreRef": map[string]any{
				"name": input.PullSecretStoreName,
				"kind": "ClusterSecretStore",
			},
			"target": map[string]any{
				"name":           "pull-secret",
				"creationPolicy": "Owner",
				"template": map[string]any{
					"type": "kubernetes.io/dockerconfigjson",
				},
			},
			"data": []any{
				map[string]any{
					"secretKey": ".dockerconfigjson",
					"remoteRef": map[string]any{
						"key": input.PullSecretGCPKey,
					},
				},
			},
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: marshal external secret: %w", err)
	}
	return raw, nil
}

func buildCertificate(input Input, clusterNS string) ([]byte, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	dnsName := fmt.Sprintf("*.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain)
	obj := map[string]any{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]any{
			"name":      "external-api-cert",
			"namespace": clusterNS,
			"labels": map[string]any{
				"gcp.managed.openshift.io/cluster-id":    input.ClusterID,
				"gcp.managed.openshift.io/managed-by":    "hc-controller",
				"gcp.managed.openshift.io/resource-type": "certificate",
			},
			"annotations": map[string]any{
				"gcp.managed.openshift.io/generation": genStr,
			},
		},
		"spec": map[string]any{
			"subject": map[string]any{
				"organizations": []any{"Red Hat - Hypershift"},
			},
			"usages":      []any{"server auth", "client auth"},
			"duration":    "2160h",
			"renewBefore": "720h",
			"privateKey": map[string]any{
				"algorithm":      "RSA",
				"encoding":       "PKCS1",
				"size":           2048,
				"rotationPolicy": "Always",
			},
			"dnsNames":   []any{dnsName},
			"secretName": "external-api-cert",
			"issuerRef": map[string]any{
				"name":  "public-issuer",
				"kind":  "ClusterIssuer",
				"group": "cert-manager.io",
			},
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: marshal certificate: %w", err)
	}
	return raw, nil
}

func buildHostedCluster(input Input, clusterNS string) ([]byte, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	apiHostname := fmt.Sprintf("api.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain)
	oauthHostname := fmt.Sprintf("oauth.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain)

	annotations := map[string]any{
		"gcp.managed.openshift.io/generation":                           genStr,
		"hypershift.openshift.io/pod-security-admission-label-override": "baseline",
		"hypershift.openshift.io/skip-kas-conflict-san-validation":      "true",
		// Inert until the HostedCluster is deleted, when HyperShift reclaims
		// guest cloud resources instead of leaving them behind.
		"hypershift.openshift.io/cleanup-cloud-resources": "true",
	}
	if input.CPOImage != "" {
		annotations["hypershift.openshift.io/control-plane-operator-image"] = input.CPOImage
	}
	if input.CAPGImage != "" {
		annotations["hypershift.openshift.io/capi-provider-gcp-image"] = input.CAPGImage
	}

	obj := map[string]any{
		"apiVersion": "hypershift.openshift.io/v1beta1",
		"kind":       "HostedCluster",
		"metadata": map[string]any{
			"name":      input.ClusterName,
			"namespace": clusterNS,
			"labels": map[string]any{
				"gcp.managed.openshift.io/cluster-id":    input.ClusterID,
				"gcp.managed.openshift.io/managed-by":    "hc-controller",
				"gcp.managed.openshift.io/resource-type": "hosted-cluster",
			},
			"annotations": annotations,
		},
		"spec": map[string]any{
			"clusterID": input.ClusterID,
			"infraID":   input.InfraID,
			"issuerURL": input.IssuerURL,
			"release": map[string]any{
				"image": input.ReleaseImage,
			},
			"channel":                      input.ReleaseChannel,
			"controllerAvailabilityPolicy": input.ControllerAvailabilityPolicy,
			"pullSecret": map[string]any{
				"name": "pull-secret",
			},
			"dns": map[string]any{
				"baseDomain":       fmt.Sprintf("in.%s-%s.%s", input.ClusterName, input.Slug, input.BaseDomain),
				"baseDomainPrefix": "",
			},
			"networking": map[string]any{
				"clusterNetwork": []any{
					map[string]any{"cidr": "10.132.0.0/14"},
				},
				"serviceNetwork": []any{
					map[string]any{"cidr": "172.31.0.0/16"},
				},
				"networkType": "OVNKubernetes",
			},
			"platform": map[string]any{
				"type": "GCP",
				"gcp":  buildGCPPlatform(input),
			},
			"services": []any{
				map[string]any{
					"service": "APIServer",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
						"route": map[string]any{
							"hostname": apiHostname,
						},
					},
				},
				map[string]any{
					"service": "OAuthServer",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
						"route": map[string]any{
							"hostname": oauthHostname,
						},
					},
				},
				map[string]any{
					"service": "Konnectivity",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
					},
				},
				map[string]any{
					"service": "Ignition",
					"servicePublishingStrategy": map[string]any{
						"type": "Route",
					},
				},
			},
			"capabilities": map[string]any{
				"disabled": []any{
					"ImageRegistry",
					"Console",
					"Ingress",
				},
			},
			"configuration": map[string]any{
				"apiServer": map[string]any{
					"servingCerts": map[string]any{
						"namedCertificates": []any{
							map[string]any{
								"names": []any{apiHostname},
								"servingCertificate": map[string]any{
									"name": "external-api-cert",
								},
							},
						},
					},
				},
				"authentication": map[string]any{
					"type": "OIDC",
					"oidcProviders": []any{
						map[string]any{
							"name": "google",
							"issuer": map[string]any{
								"issuerURL": "https://accounts.google.com",
								"audiences": []any{
									"32555940559.apps.googleusercontent.com",
								},
							},
							"claimMappings": map[string]any{
								"username": map[string]any{
									"claim": "email",
								},
								"groups": map[string]any{
									"claim":  "hd",
									"prefix": "",
								},
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: marshal hosted cluster: %w", err)
	}
	return raw, nil
}

func buildGCPPlatform(input Input) map[string]any {
	gcp := map[string]any{
		"project": input.GCPProjectID,
		"region":  input.GCPRegion,
		"networkConfig": map[string]any{
			"network": map[string]any{
				"name": input.GCPNetwork,
			},
			"privateServiceConnectSubnet": map[string]any{
				"name": input.GCPSubnet,
			},
		},
		"endpointAccess": input.GCPEndpointAccess,
		"workloadIdentity": map[string]any{
			"projectNumber": input.WIFProjectNumber,
			"poolID":        input.WIFPoolID,
			"providerID":    input.WIFProviderID,
			"serviceAccountsEmails": map[string]any{
				"nodePool":        input.NodePoolEmail,
				"controlPlane":    input.ControlPlaneEmail,
				"cloudController": input.CloudControllerEmail,
				"storage":         input.StorageEmail,
				"imageRegistry":   input.ImageRegistryEmail,
				"network":         input.NetworkEmail,
			},
		},
	}
	if len(input.ResourceLabels) > 0 {
		keys := make([]string, 0, len(input.ResourceLabels))
		for k := range input.ResourceLabels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		labels := make([]map[string]any, 0, len(keys))
		for _, k := range keys {
			labels = append(labels, map[string]any{"key": k, "value": input.ResourceLabels[k]})
		}
		gcp["resourceLabels"] = labels
	}
	return gcp
}

func buildRBACJob(input Input, hcNS string) ([]byte, error) {
	genStr := strconv.FormatInt(input.Generation, 10)
	jobName := fmt.Sprintf("rbac-setup-gen-%d", input.Generation)
	ttl := int64(300)
	backoffLimit := int32(10)
	activeDeadline := int64(1800)
	clusterAdminScript := fmt.Sprintf(`set -euo pipefail

echo "Waiting for API server to be fully ready (up to 25 minutes)..."
ATTEMPTS=0
MAX_ATTEMPTS=150

while [ $ATTEMPTS -lt $MAX_ATTEMPTS ]; do
  if kubectl --kubeconfig=/kubeconfig/kubeconfig get --raw /healthz &>/dev/null; then
    echo "API server is ready after $((ATTEMPTS * 10)) seconds"
    break
  fi
  ATTEMPTS=$((ATTEMPTS + 1))
  echo "Attempt $ATTEMPTS/$MAX_ATTEMPTS: API server not ready yet, waiting 10s..."
  sleep 10
done

if [ $ATTEMPTS -eq $MAX_ATTEMPTS ]; then
  echo "ERROR: API server did not become ready after $((MAX_ATTEMPTS * 10)) seconds"
  exit 1
fi

echo "Creating ClusterRoleBinding for redhat.com domain..."
kubectl --kubeconfig=/kubeconfig/kubeconfig apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: redhat-domain-admins
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- apiGroup: rbac.authorization.k8s.io
  kind: Group
  name: redhat.com
- apiGroup: rbac.authorization.k8s.io
  kind: User
  name: "%s"
EOF

echo "ClusterRoleBinding created successfully"

echo "Verifying ClusterRoleBinding..."
kubectl --kubeconfig=/kubeconfig/kubeconfig get clusterrolebinding redhat-domain-admins -o yaml
`, input.CreatedBy)

	obj := map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      jobName,
			"namespace": hcNS,
			"labels": map[string]any{
				"gcp.managed.openshift.io/cluster-id":    input.ClusterID,
				"gcp.managed.openshift.io/managed-by":    "hc-controller",
				"gcp.managed.openshift.io/resource-type": "rbac-setup",
				"job":                                    "rbac-setup",
			},
			"annotations": map[string]any{
				"gcp.managed.openshift.io/generation": genStr,
			},
		},
		"spec": map[string]any{
			"ttlSecondsAfterFinished": ttl,
			"backoffLimit":            backoffLimit,
			"activeDeadlineSeconds":   activeDeadline,
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{
						"job":                                 "rbac-setup",
						"gcp.managed.openshift.io/cluster-id": input.ClusterID,
					},
				},
				"spec": map[string]any{
					"serviceAccountName": "default",
					"restartPolicy":      "OnFailure",
					"containers": []any{
						map[string]any{
							"name":    "oc",
							"image":   "quay.io/openshift/origin-cli:4.20",
							"command": []any{"/bin/bash", "-c", clusterAdminScript},
							"volumeMounts": []any{
								map[string]any{
									"name":      "kubeconfig",
									"mountPath": "/kubeconfig",
									"readOnly":  true,
								},
							},
						},
					},
					"volumes": []any{
						map[string]any{
							"name": "kubeconfig",
							"secret": map[string]any{
								"secretName":  "service-network-admin-kubeconfig",
								"defaultMode": 0600,
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("hc manifest: marshal rbac job: %w", err)
	}
	return raw, nil
}
