# GCP-HCP Platform API

Kubernetes-style API server for managed hosted clusters on GCP. Uses [orlop](../orlop/) for code generation and the dual API surface (private/public field filtering via `+orlop:public` markers).

## Running Locally

The private API uses GenericAPIServer (HTTPS). The public API uses a chi.Router over plain HTTP.

### Local development

Uses Kubernetes `GenericAPIServer` for the private API — HTTPS with auto-generated self-signed certs. Auth disabled for local dev.

```sh
go run ./cmd/platform-api-server --disable-auth
```

```sh
# Private API (HTTPS, self-signed cert)
kubectl --server https://localhost:8080 --insecure-skip-tls-verify get managedhostedclusters
curl -sk https://localhost:8080/apis/gcp.managed.openshift.io/v1/namespaces/default/clusters

# Public API (HTTP)
kubectl --server http://localhost:8081 get managedhostedclusters

# k8s-native endpoints
curl -sk https://localhost:8080/healthz
curl -sk https://localhost:8080/openapi/v3
kubectl --server https://localhost:8080 --insecure-skip-tls-verify api-resources
```

### With delegated private-API auth

For testing authn/authz delegation against a real cluster (kind, minikube, GKE):

```sh
go run ./cmd/platform-api-server \
  --authentication-kubeconfig ~/.kube/config \
  --authorization-kubeconfig ~/.kube/config
```

Private-API tokens are validated via `TokenReview` and permissions checked via `SubjectAccessReview` against the target cluster's kube-apiserver. Public-API identity is supplied by ESPv2 and authorization is evaluated in-process with Cedar.

### On a cluster (Phase 5 — not yet implemented)

When deployed on GKE with an `APIService` registration, the kube-apiserver proxies requests to gecko. Clients use standard `kubectl` against the cluster — no special server URL needed.

```sh
# After APIService registration, this just works:
kubectl get managedhostedclusters -A
```

Requirements (see [api-aggregation-status.md](../api-aggregation-status.md) Phase 5):
- `APIService` manifest per API group
- TLS certs via cert-manager
- RBAC ClusterRole/ClusterRoleBinding for consumer identities
- Pod liveness/readiness probes on `/livez`, `/readyz`

## Creating a test resource

```sh
kubectl create -f test.mhc.yaml
```

Or via curl (public API):

```sh
curl -X POST http://localhost:8081/apis/gcp.managed.openshift.io/v1/namespaces/default/clusters \
  -H "Content-Type: application/json" \
  -d '{
    "apiVersion": "gcp.managed.openshift.io/v1",
    "kind": "Cluster",
    "metadata": {"name": "test-cluster"},
    "spec": {}
  }'
```

## CLI flags

| Flag | Default | Description |
|---|---|---|
| `--address` | `0.0.0.0` | Bind address |
| `--public-address` | `127.0.0.1` | Public API bind address |
| `--private-port` | `8080` | Private API port |
| `--public-port` | `8081` | Public API port |
| `--enable-public-api` | `true` | Enable public API server |
| `--cors-origins` | `*` | Comma-separated CORS origins |
| `--tls-cert-file` | | TLS cert (auto-generated if empty) |
| `--tls-key-file` | | TLS key (auto-generated if empty) |
| `--authentication-kubeconfig` | | Kubeconfig for delegated authn (in-cluster if empty) |
| `--authorization-kubeconfig` | | Kubeconfig for delegated authz (in-cluster if empty) |
| `--disable-auth` | `false` | Skip authn/authz (for testing/local dev) |
| `--dev-auth` | `false` | Accept `X-Dev-User` for public API local development only |

## Storage

Currently hardcoded to in-memory storage. Data does not survive restarts. PostgreSQL and Spanner backends exist in orlop but are not yet wired into the CLI.

## Related docs

- [Architecture](ARCHITECTURE.md) — dual API surface, conversion, storage
- [API aggregation proposal](../api-aggregation.md) — motivation and design
- [Implementation status](../api-aggregation-status.md) — phase tracker
- [orlop](../orlop/) — framework, code generator, storage backends

## Scheduler

A simple controller that assigns management clusters to ManagedHostedCluster resources:

```sh
KUBECONFIG=$PWD/kubeconfig.yaml go run ./cmd/mhc-scheduler --health-probe-bind-address=0 --metrics-bind-address=0
```

### PlatformRole discovery

Authenticated callers can `GET /apis/gcp.managed.openshift.io/v1/platformroles`
and `GET /apis/gcp.managed.openshift.io/v1/platformroles/{name}` to discover
system-managed roles and their complete permission sets. These platform-scoped
catalog reads use the same temporary authorization exemption as Version and
Channel: no project RoleBinding is required, but ESPv2 authentication remains
mandatory. Public mutation and watch operations are not supported; definitions
continue to be managed through Helm and the private API.

The existing public metadata filter applies unchanged (private-prefixed metadata
and finalizers are removed). Non-private metadata, including Argo CD annotations,
can still be returned; `gcphcpctl platformrole` minimizes metadata in its output.

## NodePool upgrades

Request immediate or one-time scheduled upgrades through `NodePool.spec.upgrade`.
The upgrade coordinator submits eligible targets through the existing
`spec.release.version` installation flow. `status.currentVersion` reports the
observed version, and `status.upgrade` plus upgrade conditions report progress.
See [NodePool upgrade flow](../controllers/nodepoolupgrade/README.md) for
request, watch, cancellation, and validation examples.
