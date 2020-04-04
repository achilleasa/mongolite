package cmd

import (
	"github.com/achilleasa/mongolite/emulator"
	"github.com/achilleasa/mongolite/emulator/backend/dummy"
	"golang.org/x/xerrors"
	"gopkg.in/urfave/cli.v2"
)

// EmulateServer implements the serve command.
func EmulateServer(ctx *cli.Context) error {
	var backend emulator.Backend

	backendType := ctx.String("backend")
	switch backendType {
	case "dummy":
		backend = dummy.NewDummyBackend()
	default:
		return xerrors.Errorf("unsupported backend %q: supported values are: dummy", backendType)
	}

	srvLogger := appLogger.WithField("backend", backend.Name())
	srvLogger.Info("emulating mongo server")

	emu, err := emulator.NewMongoEmulator(backend, srvLogger)
	if err != nil {
		return err
	}
	return startProxy(ctx, emu)
}
