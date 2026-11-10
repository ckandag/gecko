// Package replicationguard prevents public writes to replicated resources in
// configured follower regions.
package replicationguard

import (
	"fmt"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// Config describes this API server's replication mode.
//
//nolint:kubeapilinter // Middleware configuration is not a serialized API type.
type Config struct {
	Enabled      bool
	Region       string
	LeaderRegion string
	Replicate    string
	Resources    []types.ResourceInfo
}

//nolint:kubeapilinter // Internal route lookup key, not a serialized API type.
type resourceRoute struct {
	group      string
	version    string
	plural     string
	namespaced bool
}

//nolint:kubeapilinter // Internal method lookup value, not a serialized API type.
type writeOperation struct {
	verb         string
	requiresName bool
}

var writeOperations = map[string]writeOperation{
	http.MethodPost:   {verb: "create"},
	http.MethodPut:    {verb: "update", requiresName: true},
	http.MethodPatch:  {verb: "patch", requiresName: true},
	http.MethodDelete: {verb: "delete", requiresName: true},
}

var (
	rejectedWrites = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "replication",
			Name:           "readonly_write_rejections_total",
			Help:           "Public API writes rejected because this region is a replication follower.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"region", "resource_kind", "verb"},
	)
	registerMetrics sync.Once
)

// New validates the configured mode and returns a public API middleware.
func New(config Config) (func(http.Handler) http.Handler, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	var replicated map[resourceRoute]string
	if config.Enabled {
		var err error
		replicated, err = parseResources(config.Replicate, config.Resources)
		if err != nil {
			return nil, fmt.Errorf("replicate: %w", err)
		}
	}
	registerMetrics.Do(func() {
		// The private Kubernetes API already serves this registry at /metrics.
		legacyregistry.MustRegister(rejectedWrites)
	})

	return func(next http.Handler) http.Handler {
		if !config.Enabled || config.Region == config.LeaderRegion {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			resourceKind, verb, replicatedWrite := replicatedMutation(r, replicated)
			if !replicatedWrite {
				next.ServeHTTP(w, r)
				return
			}

			rejectedWrites.WithLabelValues(config.Region, resourceKind, verb).Inc()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("{\"error\":\"region is read-only\",\"retryable\":false}"))
		})
	}, nil
}

func (config Config) validate() error {
	if !config.Enabled {
		return nil
	}
	if config.Region == "" || config.LeaderRegion == "" ||
		config.Region != strings.TrimSpace(config.Region) ||
		config.LeaderRegion != strings.TrimSpace(config.LeaderRegion) {
		return fmt.Errorf("enabled replication write guard requires nonempty, unpadded region and leader region")
	}
	return nil
}

// parseResources resolves the same plural.group identifiers used by the
// replication controller against the public API's registered resources.
func parseResources(csv string, available []types.ResourceInfo) (map[resourceRoute]string, error) {
	registered := make(map[string]types.ResourceInfo, len(available))
	for _, resource := range available {
		id := resource.Plural + "." + resource.GVK.Group
		registered[id] = resource
	}

	selected := make(map[resourceRoute]string)
	for _, part := range strings.Split(csv, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		resource, ok := registered[id]
		if !ok {
			return nil, fmt.Errorf("unsupported public replicated resource %q", id)
		}
		selected[resourceRoute{
			group: resource.GVK.Group, version: resource.GVK.Version,
			plural: resource.Plural, namespaced: resource.Namespaced,
		}] = resource.GVK.Kind
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("at least one replicated resource is required")
	}
	return selected, nil
}

// replicatedMutation matches mutating public routes for configured resources.
func replicatedMutation(r *http.Request, replicated map[resourceRoute]string) (resourceKind, verb string, matched bool) {
	if r == nil || r.URL == nil {
		return "", "", false
	}
	rawPath := r.URL.Path
	if !strings.HasPrefix(rawPath, "/") || path.Clean(rawPath) != rawPath || strings.Contains(rawPath, "..") {
		return "", "", false
	}

	parts := strings.Split(strings.TrimPrefix(rawPath, "/"), "/")
	if len(parts) < 4 || parts[0] != "apis" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}

	route := resourceRoute{group: parts[1], version: parts[2]}
	var named bool
	switch {
	case (len(parts) == 6 || len(parts) == 7) && parts[3] == "namespaces" && parts[4] != "":
		route.namespaced = true
		route.plural = parts[5]
		named = len(parts) == 7
	case len(parts) == 4 || len(parts) == 5:
		route.plural = parts[3]
		named = len(parts) == 5
	default:
		return "", "", false
	}
	if route.plural == "" || named && parts[len(parts)-1] == "" {
		return "", "", false
	}
	resourceKind, ok := replicated[route]
	if !ok {
		return "", "", false
	}
	operation, ok := writeOperations[r.Method]
	if !ok || named != operation.requiresName {
		return "", "", false
	}
	return resourceKind, operation.verb, true
}
