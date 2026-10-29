package replication

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	CreateOrUpdate = "CREATE_OR_UPDATE"
	Delete         = "DELETE"
	Inventory      = "INVENTORY"
	ResyncRequest  = "RESYNC_REQUEST"

	// Leave room for Pub/Sub's message framing and attributes.
	MaxEventBytes = 1 << 20
)

// Event carries source-assigned versions. SnapshotResourceVersion is the LIST
// boundary, while SourceResourceVersion is the object's own mutation version.
type Event struct {
	EventType               string          `json:"eventType"`
	ResourceKind            string          `json:"resourceKind"`
	OriginRegion            string          `json:"originRegion"`
	Generation              uint64          `json:"generation"`
	SourceResourceVersion   string          `json:"sourceResourceVersion,omitempty"`
	SnapshotResourceVersion string          `json:"snapshotResourceVersion,omitempty"`
	Namespace               string          `json:"namespace,omitempty"`
	Name                    string          `json:"name,omitempty"`
	UpdatedAt               time.Time       `json:"updatedAt"`
	SyncID                  string          `json:"syncID,omitempty"`
	Object                  json.RawMessage `json:"object,omitempty"`
	PageIndex               uint32          `json:"pageIndex"`
	PageCount               uint32          `json:"pageCount"`
	Inventory               []ObjectRef     `json:"inventory,omitempty"`
}

type ObjectRef struct {
	Namespace             string `json:"namespace"`
	Name                  string `json:"name"`
	SourceResourceVersion string `json:"sourceResourceVersion"`
	SourceUID             string `json:"sourceUID"`
}

type Resource struct {
	ID  string
	GVR schema.GroupVersionResource
}

var supportedResources = map[string]schema.GroupVersionResource{
	"roles.gcp.managed.openshift.io":        {Group: "gcp.managed.openshift.io", Version: "v1", Resource: "roles"},
	"rolebindings.gcp.managed.openshift.io": {Group: "gcp.managed.openshift.io", Version: "v1", Resource: "rolebindings"},
}

// ParseResources accepts registered group/resource identifiers. Adding a type
// requires an explicit metadata and lifecycle contract on the receiving side.
func ParseResources(csv string) ([]Resource, error) {
	var resources []Resource
	seen := make(map[string]bool)
	for _, part := range strings.Split(csv, ",") {
		id := strings.TrimSpace(part)
		if id == "" {
			continue
		}
		gvr, ok := supportedResources[id]
		if !ok {
			return nil, fmt.Errorf("unsupported replicated resource %q", id)
		}
		if !seen[id] {
			resources = append(resources, Resource{ID: id, GVR: gvr})
			seen[id] = true
		}
	}
	if len(resources) == 0 {
		return nil, fmt.Errorf("at least one replicated resource is required")
	}
	return resources, nil
}
