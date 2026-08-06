// Package driver registers Verglas as a live Rill OLAP connector.
package driver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rilldata/rill/runtime/drivers"
	"github.com/rilldata/rill/runtime/pkg/activity"
	"github.com/rilldata/rill/runtime/storage"
	"go.uber.org/zap"
)

func init() {
	d := connectorDriver{}
	drivers.Register("verglas", d)
	drivers.RegisterAsConnector("verglas", d)
}

var connectorSpec = drivers.Spec{
	DisplayName: "Verglas",
	Description: "Run live analytical queries through the Verglas query engine.",
	ConfigProperties: []*drivers.PropertySpec{
		{
			Key:         "endpoint",
			Type:        drivers.StringPropertyType,
			Required:    true,
			DisplayName: "Verglas endpoint",
			Placeholder: "http://verglas-server:8334",
		},
		{
			Key:         "token",
			Type:        drivers.StringPropertyType,
			DisplayName: "Bearer token",
			Secret:      true,
		},
	},
	ImplementsOLAP: true,
}

type connectorDriver struct{}

// Spec describes the live OLAP connector to Rill.
func (connectorDriver) Spec() drivers.Spec {
	return connectorSpec
}

// Open validates connector properties and builds a stateless HTTP connection.
func (connectorDriver) Open(_ string, _ string, properties map[string]any, _ *storage.Client, _ *activity.Client, logger *zap.Logger) (drivers.Handle, error) {
	endpoint, _ := properties["endpoint"].(string)
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("verglas connector requires endpoint")
	}
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		return nil, fmt.Errorf("verglas endpoint must use http:// or https://")
	}
	token, _ := properties["token"].(string)
	return newConnection(config{Endpoint: endpoint, Token: token}, logger), nil
}

// HasAnonymousSourceAccess reports that no source ingestion path exists.
func (connectorDriver) HasAnonymousSourceAccess(context.Context, map[string]any, *zap.Logger) (bool, error) {
	return false, nil
}

// TertiarySourceConnectors reports that Verglas needs no secondary connector.
func (connectorDriver) TertiarySourceConnectors(context.Context, map[string]any, *zap.Logger) ([]string, error) {
	return nil, nil
}

type config struct {
	Endpoint string
	Token    string
}

type connection struct {
	config config
	client *http.Client
	logger *zap.Logger
}

func newConnection(cfg config, logger *zap.Logger) *connection {
	return &connection{
		config: cfg,
		client: &http.Client{Timeout: 5 * time.Minute},
		logger: logger,
	}
}

// Ping proves that the endpoint can execute a query.
func (c *connection) Ping(ctx context.Context) error {
	result, err := c.Query(ctx, &drivers.Statement{Query: "SELECT 1"})
	if err != nil {
		return err
	}
	return result.Close()
}

// Driver returns the registered connector name.
func (c *connection) Driver() string { return "verglas" }

// Config returns the non-derived connector properties.
func (c *connection) Config() map[string]any {
	return map[string]any{"endpoint": c.config.Endpoint, "token": c.config.Token}
}

// Migrate is a no-op because the connector owns no state.
func (c *connection) Migrate(context.Context) error { return nil }

// MigrationStatus reports the stateless connector at its only schema level.
func (c *connection) MigrationStatus(context.Context) (int, int, error) { return 0, 0, nil }

// Close releases no server-side or local persistent state.
func (c *connection) Close() error { return nil }

// AsRegistry rejects registry use.
func (c *connection) AsRegistry() (drivers.RegistryStore, bool) { return nil, false }

// AsCatalogStore rejects Rill catalog storage use.
func (c *connection) AsCatalogStore(string) (drivers.CatalogStore, bool) { return nil, false }

// AsRepoStore rejects Rill project storage use.
func (c *connection) AsRepoStore(string) (drivers.RepoStore, bool) { return nil, false }

// AsAdmin rejects control-plane administration use.
func (c *connection) AsAdmin(string) (drivers.AdminService, bool) { return nil, false }

// AsAI rejects AI service use.
func (c *connection) AsAI(string) (drivers.AIService, bool) { return nil, false }

// AsOLAP exposes the live query implementation.
func (c *connection) AsOLAP(string) (drivers.OLAPStore, bool) { return c, true }

// AsInformationSchema exposes live catalog introspection.
func (c *connection) AsInformationSchema() (drivers.InformationSchema, bool) { return c, true }

// AsObjectStore rejects object-store use; all bytes stay behind Verglas.
func (c *connection) AsObjectStore() (drivers.ObjectStore, bool) { return nil, false }

// AsFileStore rejects file-store use.
func (c *connection) AsFileStore() (drivers.FileStore, bool) { return nil, false }

// AsWarehouse rejects warehouse export use.
func (c *connection) AsWarehouse() (drivers.Warehouse, bool) { return nil, false }

// AsModelExecutor prevents Rill from materializing models in Verglas.
func (c *connection) AsModelExecutor(string, *drivers.ModelExecutorOptions) (drivers.ModelExecutor, error) {
	return nil, drivers.ErrNotImplemented
}

// AsModelManager prevents Rill from managing materialized model state.
func (c *connection) AsModelManager(string) (drivers.ModelManager, error) {
	return nil, drivers.ErrNotImplemented
}

// AsNotifier rejects notification use.
func (c *connection) AsNotifier(map[string]any) (drivers.Notifier, error) {
	return nil, drivers.ErrNotNotifier
}
