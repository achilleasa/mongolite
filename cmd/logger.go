package cmd

import (
	"os"

	"gopkg.in/Sirupsen/logrus.v1"
	"gopkg.in/urfave/cli.v2"
)

var (
	rootLogger = logrus.New()
	appLogger  = rootLogger.WithField("module", "app")
)

// SetupLogger is invoked by the cli before a command is executed.
func SetupLogger(*cli.Context) error {
	rootLogger.SetOutput(os.Stderr)
	return nil
}

// ExitErrorHandler is invoked when the cli encounters a fatal error.
func ExitErrorHandler(err error) {
	appLogger.WithError(err).Errorf("terminating due to error")
	os.Exit(1)
}
