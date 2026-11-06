package aggregated

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"

	pkgschema "github.com/openshift-online/gecko/orlop/pkg/apiserver/schema"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/types"

	apiext "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	apiopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/util/compatibility"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	basecompatibility "k8s.io/component-base/compatibility"

	"sigs.k8s.io/yaml"
)

// AggregatedServer wraps a GenericAPIServer to serve aggregated API resources.
type AggregatedServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

// PreparedAggregatedServer is a server that has been prepared to run.
type PreparedAggregatedServer struct {
	*AggregatedServer
}

// New creates a new AggregatedServer from a CompletedConfig.
func New(c CompletedConfig) (*AggregatedServer, error) {
	scheme := c.Scheme

	// Register meta types needed by the generic API server.
	metav1.AddToGroupVersion(scheme, runtimeschema.GroupVersion{Version: "v1"})
	if err := metainternalversion.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("failed to add metainternalversion to scheme: %w", err)
	}

	codecs := serializer.NewCodecFactory(scheme)
	parameterCodec := runtime.NewParameterCodec(scheme)

	// Set up secure serving via options.
	servingOptions := options.NewSecureServingOptions()
	servingOptions.BindAddress = net.ParseIP(c.BindAddress)
	servingOptions.BindPort = c.BindPort
	if c.CertFile != "" && c.KeyFile != "" {
		servingOptions.ServerCert.CertKey = options.CertKey{
			CertFile: c.CertFile,
			KeyFile:  c.KeyFile,
		}
	} else {
		// Generate self-signed certs for the loopback address.
		if err := servingOptions.MaybeDefaultWithSelfSignedCerts("localhost", nil, nil); err != nil {
			return nil, fmt.Errorf("failed to generate self-signed certs: %w", err)
		}
	}

	// Register component globals for version info (required by GenericAPIServer.Complete).
	if compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent) == nil {
		effectiveVersion := compatibility.DefaultBuildEffectiveVersion()
		utilruntime.Must(compatibility.DefaultComponentGlobalsRegistry.Register(basecompatibility.DefaultKubeComponent, effectiveVersion, utilfeature.DefaultMutableFeatureGate))
	}

	serverConfig := genericapiserver.NewConfig(codecs)
	serverConfig.EffectiveVersion = compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent)

	if err := servingOptions.WithLoopback().ApplyToConfig(serverConfig); err != nil {
		return nil, fmt.Errorf("failed to apply secure serving options: %w", err)
	}

	// Configure authentication and authorization.
	if c.DisableAuth {
		serverConfig.Authorization.Authorizer = authorizerfactory.NewAlwaysAllowAuthorizer()
	} else {
		authnOpts := options.NewDelegatingAuthenticationOptions()
		authnOpts.RemoteKubeConfigFile = c.AuthenticationKubeconfig
		authnOpts.RemoteKubeConfigFileOptional = (c.AuthenticationKubeconfig == "")
		authnOpts.SkipInClusterLookup = false
		authnOpts.TolerateInClusterLookupFailure = true

		if err := authnOpts.ApplyTo(&serverConfig.Authentication, serverConfig.SecureServing, serverConfig.OpenAPIConfig); err != nil {
			return nil, fmt.Errorf("failed to apply authentication options: %w", err)
		}

		authzOpts := options.NewDelegatingAuthorizationOptions()
		authzOpts.RemoteKubeConfigFile = c.AuthorizationKubeconfig
		authzOpts.RemoteKubeConfigFileOptional = (c.AuthorizationKubeconfig == "")
		authzOpts.AlwaysAllowPaths = []string{"/healthz", "/readyz", "/livez"}
		authzOpts.AlwaysAllowGroups = []string{"system:masters"}

		if err := authzOpts.ApplyTo(&serverConfig.Authorization); err != nil {
			return nil, fmt.Errorf("failed to apply authorization options: %w", err)
		}
	}

	// Configure OpenAPI v2 and v3.
	defNamer := apiopenapi.NewDefinitionNamer(scheme)
	getDefinitions := buildOpenAPIDefinitions(scheme, c.Resources)
	serverConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(getDefinitions, defNamer)
	serverConfig.OpenAPIConfig.Info.Title = "Gecko Aggregated API"
	serverConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(getDefinitions, defNamer)
	serverConfig.OpenAPIV3Config.Info.Title = "Gecko Aggregated API"

	// Register health checks.
	for _, hc := range c.HealthCheckers {
		serverConfig.AddHealthChecks(hc)
	}

	// Complete the config (nil informers - we don't need them for a lean setup).
	completedServerConfig := serverConfig.Complete(nil)

	genericServer, err := completedServerConfig.New("gecko-aggregated", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("failed to create generic API server: %w", err)
	}

	// Group resources by API group and version.
	groups := make(map[string]map[string][]types.ResourceInfo)
	for _, res := range c.Resources {
		g := res.GVK.Group
		v := res.GVK.Version
		if groups[g] == nil {
			groups[g] = make(map[string][]types.ResourceInfo)
		}
		groups[g][v] = append(groups[g][v], res)
	}

	// Register internal (hub) versions for each type so the field manager
	// can convert between versions for SSA field tracking.
	registeredInternal := make(map[string]bool)
	for _, res := range c.Resources {
		key := res.GVK.Group + "/" + res.GVK.Kind
		if registeredInternal[key] {
			continue
		}
		registeredInternal[key] = true
		internalGV := runtimeschema.GroupVersion{Group: res.GVK.Group, Version: runtime.APIVersionInternal}
		if obj, err := scheme.New(res.GVK); err == nil {
			scheme.AddKnownTypes(internalGV, obj)
		}
		listGVK := res.GVK.GroupVersion().WithKind(res.GVK.Kind + "List")
		if listObj, err := scheme.New(listGVK); err == nil {
			scheme.AddKnownTypes(internalGV, listObj)
		}
	}

	// Install each API group.
	for group, versions := range groups {
		apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(group, scheme, parameterCodec, codecs)

		// Sort versions so the shared store is always created with a
		// deterministic GVK regardless of map iteration order.
		sortedVersions := make([]string, 0, len(versions))
		for v := range versions {
			sortedVersions = append(sortedVersions, v)
		}
		sort.Strings(sortedVersions)

		for _, version := range sortedVersions {
			resources := versions[version]
			storageMap := make(map[string]rest.Storage)
			for _, info := range resources {

				// Create schema processor from SchemaYAML.
				processor, err := createProcessor(info.SchemaYAML)
				if err != nil {
					return nil, fmt.Errorf("failed to create processor for %s: %w", info.Plural, err)
				}

				strategy := NewResourceStrategy(scheme, processor, info.Namespaced, info.GVK, c.Logger.WithValues("resource", info.Plural))

				resourceType := strings.ToLower(info.GVK.Group + "_" + info.GVK.Kind)
				store, err := c.StorageFactory(resourceType, scheme, info.GVK)
				if err != nil {
					return nil, fmt.Errorf("failed to create store for %s: %w", info.Plural, err)
				}
				resourceStorage := NewResourceStorage(store, strategy, info.GVK, info.Plural, info.Singular, scheme, c.Logger.WithValues("resource", info.Plural), info.PrinterColumns)
				storageMap[info.Plural] = resourceStorage
				storageMap[info.Plural+"/status"] = NewStatusStorage(resourceStorage)
			}
			apiGroupInfo.VersionedResourcesStorageMap[version] = storageMap
		}

		if err := genericServer.InstallAPIGroup(&apiGroupInfo); err != nil {
			return nil, fmt.Errorf("failed to install API group %s: %w", group, err)
		}
	}

	return &AggregatedServer{GenericAPIServer: genericServer}, nil
}

// PrepareRun prepares the server to run.
func (s *AggregatedServer) PrepareRun() *PreparedAggregatedServer {
	return &PreparedAggregatedServer{AggregatedServer: s}
}

// RunWithContext runs the prepared server until ctx is canceled.
func (s *PreparedAggregatedServer) RunWithContext(ctx context.Context) error {
	return s.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}

// createProcessor creates a schema processor from YAML schema.
// This replicates the logic from ResourceRegistry.createProcessor (which is unexported).
func createProcessor(schemaYAML string) (*pkgschema.Processor, error) {
	var propsV1 apiextv1.JSONSchemaProps
	if err := yaml.Unmarshal([]byte(schemaYAML), &propsV1); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema YAML: %w", err)
	}

	var props apiext.JSONSchemaProps
	if err := apiextv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&propsV1, &props, nil); err != nil {
		return nil, fmt.Errorf("failed to convert schema: %w", err)
	}

	structural, err := structuralschema.NewStructural(&props)
	if err != nil {
		return nil, fmt.Errorf("failed to create structural schema: %w", err)
	}

	return pkgschema.NewProcessor(structural, &props)
}
