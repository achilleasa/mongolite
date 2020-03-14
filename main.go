package main

import (
	"github.com/achilleasa/mongolite/cmd"
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
				},
			},
		},
		Before:         cmd.SetupLogger,
		ExitErrHandler: cmd.ExitErrorHandler,
	}

	app.RunAndExitOnError()
}
