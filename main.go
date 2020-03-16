package main

import (
	"strings"

	"github.com/achilleasa/mongolite/cmd"
	"github.com/achilleasa/mongolite/protocol"
	"gopkg.in/urfave/cli.v2"
)

func main() {
	app := &cli.App{
		Name:  "mongolite",
		Usage: "An experimental proxy that speaks the mongodb wire protocol and uses sqlite as a backend",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "listen-address", Value: ":37017", Usage: "the address to listen for incoming client connections"},
			&cli.StringFlag{Name: "listen-tls-file", Value: "", Usage: "path to a file with a TLS cert/pk for the server if TLS support should be enabled"},
			&cli.StringFlag{Name: "listen-tls-file-password", Value: "", Usage: "password for decrypting TLS cert/pk data"},
		},
		Commands: []*cli.Command{
			&cli.Command{
				Name:  "tools",
				Usage: "Helper tools",
				Subcommands: []*cli.Command{
					&cli.Command{
						Name:  "proxy",
						Usage: "Proxy (and optionaly record) incoming connections to a remote mongod instance",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "remote-address", Value: "127.0.0.1:27017", Usage: "the address of a remote mongod instance to proxy connections to"},
							&cli.StringFlag{Name: "remote-tls-file", Value: "", Usage: "path to a file with a TLS cert/pk for the remote mongod if TLS support should be enabled"},
							&cli.StringFlag{Name: "remote-tls-file-password", Value: "", Usage: "password for decrypting TLS cert/pk data"},
							&cli.BoolFlag{Name: "remote-tls-no-verify", Usage: "skip TLS verification when connecting to remote mongod"},
							&cli.StringFlag{Name: "rec-requests-to", Value: "", Usage: "a filename for recroding client requests (only if specified)"},
							&cli.StringFlag{Name: "rec-responses-to", Value: "", Usage: "a filename for recording server responses (only if specified)"},
						},
						Action:   cmd.ProxyToRemote,
						Category: "tools",
					},
					&cli.Command{
						Name:      "analyze",
						ArgsUsage: "FILE",
						Usage:     "Decode and analyze recorded requests",
						Description: `
Decode and analyze recorded mongo client request stream from FILE. If a value 
of '-' is provided for the FILE argument, the tool will read the request stream
from STDIN`,
						Flags: []cli.Flag{
							&cli.IntFlag{Name: "offset", Value: 0, Usage: "number of request entries to skip"},
							&cli.IntFlag{Name: "limit", Value: 0, Usage: "number of request entries to display; if 0 all entries will be displayed"},
							&cli.StringSliceFlag{Name: "filter", Usage: "only show requests of `TYPE`. Supported types: " + strings.Join(protocol.AllRequestTypeNames(), ", ")},
						},
						Action:   cmd.AnalyzeStream,
						Category: "tools",
					},
				},
			},
		},
		Before:         cmd.SetupLogger,
		ExitErrHandler: cmd.ExitErrorHandler,
	}

	app.RunAndExitOnError()
}
