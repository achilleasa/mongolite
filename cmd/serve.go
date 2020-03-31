package cmd

import (
	"github.com/achilleasa/mongolite/proxy/handler"
	"golang.org/x/xerrors"
	"gopkg.in/urfave/cli.v2"
)

// EmulateServer implements the serve command.
func EmulateServer(ctx *cli.Context) error {
	var backend handler.Backend

	backendType := ctx.String("backend")
	switch backendType {
	case "none":
	default:
		return xerrors.Errorf("unsupported backend %q: supported values are: none", backendType)
	}

	appLogger.WithField("backend", backendType).Info("emulating mongo server")
	return startProxy(ctx, handler.NewMongoEmulator(backend))
}
