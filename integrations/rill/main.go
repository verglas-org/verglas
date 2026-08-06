// The Rill CLI with the Verglas live OLAP connector registered.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/rilldata/rill/cli/cmd"
	"github.com/rilldata/rill/cli/pkg/version"
	_ "github.com/verglas-org/verglas/integrations/rill/driver"
)

var (
	versionNumber = "0.88.6-verglas"
	commit        = ""
	buildDate     = ""
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	cmd.Run(ctx, version.Version{Number: versionNumber, Commit: commit, Timestamp: buildDate})
}
