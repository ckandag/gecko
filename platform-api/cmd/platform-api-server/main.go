package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	_ "github.com/lib/pq"

	"github.com/openshift-online/gecko/orlop/pkg/apiserver"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/memory"
	"github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/postgres"
	spannerbackend "github.com/openshift-online/gecko/orlop/pkg/apiserver/storage/spanner"
	privatev1 "github.com/openshift-online/gecko/platform-api/api/private/v1"
	"github.com/openshift-online/gecko/platform-api/pkg/authn"
	"github.com/openshift-online/gecko/platform-api/pkg/authz"
	"github.com/openshift-online/gecko/platform-api/pkg/featureflags"

	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

func main() {
	var (
		address         string
		publicAddress   string
		privatePort     int
		publicPort      int
		corsOrigins     string
		enablePublic    bool
		tlsCertFile     string
		tlsKeyFile      string
		authnKubeconfig string
		authzKubeconfig string
		disableAuth     bool
		devAuth         bool
		migrateOnly     bool
	)

	flag.StringVar(&address, "address", "0.0.0.0", "address to bind to")
	flag.StringVar(&publicAddress, "public-address", "127.0.0.1", "address to bind the public API to")
	flag.IntVar(&privatePort, "private-port", 8080, "port for private API")
	flag.IntVar(&publicPort, "public-port", 8081, "port for public API")
	flag.BoolVar(&enablePublic, "enable-public-api", true, "enable public API server")
	flag.StringVar(&corsOrigins, "cors-origins", "*", "comma-separated list of allowed CORS origins")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "path to TLS certificate (auto-generated if empty)")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "path to TLS private key (auto-generated if empty)")
	flag.StringVar(&authnKubeconfig, "authentication-kubeconfig", "", "kubeconfig for delegated authentication (in-cluster if empty)")
	flag.StringVar(&authzKubeconfig, "authorization-kubeconfig", "", "kubeconfig for delegated authorization (in-cluster if empty)")
	flag.BoolVar(&disableAuth, "disable-auth", false, "disable authentication/authorization (for testing/local dev)")
	flag.BoolVar(&devAuth, "dev-auth", false, "accept X-Dev-User for public API authentication (local development only)")
	flag.BoolVar(&migrateOnly, "migrate-only", false, "run Spanner DDL migrations and exit (for PreSync Jobs)")
	flag.Parse()

	logger := stdr.New(nil)

	// --migrate-only: run Spanner DDL migrations with retry and exit.
	// Designed for use as an ArgoCD PreSync Job with elevated IAM
	// permissions (roles/spanner.databaseAdmin) so the Deployment can
	// run with only roles/spanner.databaseUser.
	if migrateOnly {
		spannerDB := os.Getenv("SPANNER_DATABASE")
		if spannerDB == "" {
			log.Fatal("--migrate-only requires SPANNER_DATABASE to be set")
		}
		tablePrefix := os.Getenv("SPANNER_TABLE_PREFIX")

		log.Printf("Running Spanner DDL migrations: %s", spannerDB)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		if err := spannerbackend.RunMigrations(ctx, spannerDB, tablePrefix, nil, logger); err != nil {
			log.Fatalf("Spanner DDL migration failed: %v", err)
		}

		log.Println("Spanner DDL migrations completed successfully")
		os.Exit(0)
	}

	if err := validatePublicAuthAddress(enablePublic, address, publicAddress, devAuth, disableAuth); err != nil {
		log.Fatal(err)
	}

	featureFlagEvaluator, err := featureflags.NewFromEnvironment()
	if err != nil {
		log.Fatalf("failed to initialize feature flags: %v", err)
	}
	defer featureFlagEvaluator.Shutdown()

	// Parse CORS origins
	origins := []string{}
	if corsOrigins != "" {
		origins = strings.Split(corsOrigins, ",")
		for i, origin := range origins {
			origins[i] = strings.TrimSpace(origin)
		}
	}

	// Configure storage backend
	// Priority: Spanner (SPANNER_DATABASE set) > PostgreSQL (DB_HOST set) > in-memory (default)
	var storageFactory apiserver.StorageFactory
	if spannerDB := os.Getenv("SPANNER_DATABASE"); spannerDB != "" {
		// SPANNER_DATABASE must be a full Spanner database resource path:
		//   projects/<project>/instances/<instance>/databases/<database>
		// Set SPANNER_EMULATOR_HOST to redirect to a local emulator, e.g.:
		//   SPANNER_EMULATOR_HOST=localhost:9010
		tablePrefix := os.Getenv("SPANNER_TABLE_PREFIX")

		// SKIP_DDL_SETUP is set by the Helm chart when an ArgoCD PreSync
		// Job handles DDL migrations. The Job runs with
		// roles/spanner.databaseAdmin; the Deployment only needs
		// roles/spanner.databaseUser for data operations.
		skipDDL := os.Getenv("SKIP_DDL_SETUP") == "true"
		if skipDDL {
			log.Println("Skipping DDL setup (handled by PreSync Job)")
		}

		log.Printf("Connecting to Spanner database: %s", spannerDB)

		factory, err := spannerbackend.NewStorageFactory(spannerbackend.StorageFactoryConfig{
			Database:    spannerDB,
			TablePrefix: tablePrefix,
			Context:     context.Background(),
			Logger:      logger,
			SkipDDL:     skipDDL,
		})
		if err != nil {
			log.Fatalf("Failed to create Spanner storage factory: %v", err)
		}

		log.Printf("Connected to Spanner: %s", spannerDB)
		storageFactory = factory
	} else if dbHost := os.Getenv("DB_HOST"); dbHost != "" {
		dbPort := os.Getenv("DB_PORT")
		if dbPort == "" {
			dbPort = "5432"
		}
		dbName := os.Getenv("DB_NAME")
		if dbName == "" {
			dbName = "orlop"
		}
		dbUser := os.Getenv("DB_USER")
		if dbUser == "" {
			dbUser = "orlop"
		}
		dbPassword := os.Getenv("DB_PASSWORD")
		dbSSLMode := os.Getenv("DB_SSLMODE")
		if dbSSLMode == "" {
			dbSSLMode = "disable"
		}

		connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
			dbHost, dbPort, dbName, dbUser, dbPassword, dbSSLMode)

		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		if err := db.PingContext(context.Background()); err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}

		log.Printf("Connected to PostgreSQL at %s:%s/%s", dbHost, dbPort, dbName)

		storageFactory = postgres.NewStorageFactory(postgres.StorageFactoryConfig{
			DB:         db,
			ConnString: connStr,
			Context:    context.Background(),
		})
	} else {
		// The API server and storage-backed validators must see the same objects
		// when running without an external database.
		storageFactory = newSharedMemoryStorageFactory()
		log.Println("No SPANNER_DATABASE or DB_HOST set, using in-memory storage")
	}

	// Create server with resource configuration
	privateScheme := getPrivateScheme()
	stores, err := authz.NewStores(storageFactory, privateScheme)
	if err != nil {
		log.Fatalf("Failed to create authorization stores: %v", err)
	}
	upgradeValidator, err := newNodePoolUpgradeValidator(storageFactory, privateScheme)
	if err != nil {
		log.Fatalf("Failed to create NodePool upgrade validator: %v", err)
	}
	privatev1.SetValidatorDeps(privatev1.ValidatorDeps{
		RoleExists:                     stores.RoleExists,
		PlatformRoleExists:             stores.PlatformRoleExists,
		ValidateNodePoolUpgradeRequest: upgradeValidator.Validate,
	})

	publicResources := getPublicResources()
	publicMiddlewareFactory := func(_ apiserver.StorageFactory, _ *runtime.Scheme, stopCh <-chan struct{}) ([]func(http.Handler) http.Handler, error) {
		if disableAuth {
			return nil, nil
		}

		authorizer, err := authz.NewAuthorizer(context.Background(), stores, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load authorization policy: %w", err)
		}
		authorizer.StartWatching(stopCh)
		authorization := authz.Middleware(authorizer, logger, publicResources)
		return []func(http.Handler) http.Handler{
			authn.Middleware(authn.Config{AllowDevHeader: devAuth}),
			conditionalAuthorizationMiddleware(featureFlagEvaluator, authorization, logger),
		}, nil
	}

	opts := apiserver.Options{
		Address: address,
		Private: apiserver.PrivateAPIOptions{
			Port:                     privatePort,
			Resources:                getPrivateResources(),
			Scheme:                   privateScheme,
			TLSCertFile:              tlsCertFile,
			TLSKeyFile:               tlsKeyFile,
			AuthenticationKubeconfig: authnKubeconfig,
			AuthorizationKubeconfig:  authzKubeconfig,
			DisableAuth:              disableAuth,
		},
		Public: apiserver.PublicAPIOptions{
			Enable:            enablePublic,
			Address:           publicAddress,
			Port:              publicPort,
			Resources:         publicResources,
			Scheme:            getPublicScheme(),
			MiddlewareFactory: publicMiddlewareFactory,
		},
		StorageFactory: storageFactory,
		CORSOrigins:    origins,
		Logger:         logger,
	}

	server, err := apiserver.New(opts)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		if err := server.Run(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for signal
	<-sigChan
	log.Println("Shutting down server...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}

func newSharedMemoryStorageFactory() apiserver.StorageFactory {
	type memoryStoreKey struct {
		resourceType string
		scheme       *runtime.Scheme
		gvk          runtimeschema.GroupVersionKind
	}
	var memoryStores sync.Map
	return func(resourceType string, scheme *runtime.Scheme, gvk runtimeschema.GroupVersionKind) (storage.ResourceStore, error) {
		key := memoryStoreKey{resourceType: resourceType, scheme: scheme, gvk: gvk}
		store, _ := memoryStores.LoadOrStore(key, memory.NewMemoryStore(resourceType, scheme, gvk))
		return store.(storage.ResourceStore), nil
	}
}

const publicAuthorizationEnabledFlag = "gecko.public-api.authorization.enabled"

type booleanFeatureFlagEvaluator interface {
	Boolean(context.Context, string, bool) (bool, error)
}

func conditionalAuthorizationMiddleware(evaluator booleanFeatureFlagEvaluator, authorization func(http.Handler) http.Handler, logger logr.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		enforced := authorization(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			enforce, err := evaluator.Boolean(r.Context(), publicAuthorizationEnabledFlag, true)
			if err != nil {
				logger.Error(err, "authorization feature flag evaluation failed; enforcing authorization")
				enforce = true
			}
			if enforce {
				enforced.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func validatePublicAuthAddress(enablePublic bool, address, publicAddress string, devAuth, disableAuth bool) error {
	if !enablePublic || (!devAuth && !disableAuth) {
		return nil
	}

	effectiveAddress := publicAddress
	if effectiveAddress == "" {
		effectiveAddress = address
		if disableAuth {
			// aggregated.Config.Complete forces the private bind address to
			// loopback when auth is disabled, and the public server defaults
			// to that resolved address when --public-address is empty.
			effectiveAddress = "127.0.0.1"
		}
		if effectiveAddress == "" {
			effectiveAddress = "0.0.0.0"
		}
	}

	if isLoopbackAddress(effectiveAddress) {
		return nil
	}

	mode := "--dev-auth"
	if disableAuth {
		mode = "--disable-auth"
		if devAuth {
			mode = "--dev-auth and --disable-auth"
		}
	}
	return fmt.Errorf("%s requires the public API to bind to a loopback address", mode)
}

func isLoopbackAddress(address string) bool {
	address = strings.TrimSpace(address)
	if strings.EqualFold(address, "localhost") {
		return true
	}
	address = strings.Trim(address, "[]")
	ip := net.ParseIP(address)
	return ip != nil && ip.IsLoopback()
}
