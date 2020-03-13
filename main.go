package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/achilleasa/mongolite/handler"
	"github.com/achilleasa/mongolite/proxy"
	"golang.org/x/xerrors"
	"gopkg.in/Sirupsen/logrus.v1"
	"gopkg.in/urfave/cli.v2"
)

var (
	rootLogger = logrus.New()
	appLogger  = rootLogger.WithField("module", "app")
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
						Usage: "Proxy incoming connections to a remote mongod instance",
						Flags: []cli.Flag{
							&cli.StringFlag{Name: "remote-address", Value: "127.0.0.1:27017", Usage: "the address of a remote mongod instance to proxy connections to"},
							&cli.StringFlag{Name: "remote-tls-file", Value: "", Usage: "path to a file with a TLS cert/pk for the remote mongod if TLS support should be enabled"},
							&cli.StringFlag{Name: "remote-tls-file-password", Value: "", Usage: "password for decrypting TLS cert/pk data"},
							&cli.BoolFlag{Name: "remote-tls-no-verify", Usage: "skip TLS verification when connecting to remote mongod"},
						},
						Action: proxyToRemote,
					},
				},
			},
		},
	}

	rootLogger.SetOutput(os.Stderr)
	if err := app.Run(os.Args); err != nil {
		appLogger.WithError(err).Errorf("terminating due to error")
		os.Exit(1)
	}
}

func proxyToRemote(ctx *cli.Context) error {
	mongoHandler, err := makeRemoteMongoHandler(ctx)
	if err != nil {
		return err
	}
	return startProxy(ctx, mongoHandler)
}

func makeRemoteMongoHandler(ctx *cli.Context) (proxy.RequestHandler, error) {
	var (
		remoteTlsConf *tls.Config
		err           error
	)
	if pemFile := ctx.String("remote-tls-file"); pemFile != "" {
		if remoteTlsConf, err = parseMongoPemFile(pemFile, ctx.String("remote-tls-file-password")); err != nil {
			return nil, err
		}

		if ctx.Bool("remote-tls-no-verify") {
			appLogger.Warn("disabling TLS verification when connecting to remote mongod")
			remoteTlsConf.InsecureSkipVerify = true
		}
	}

	return handler.NewRemoteMongoHandler(
		ctx.String("remote-address"),
		remoteTlsConf,
	)
}

func startProxy(ctx *cli.Context, reqHandler proxy.RequestHandler) error {
	var (
		proxyTlsConf *tls.Config
		err          error
	)

	if pemFile := ctx.String("listen-tls-file"); pemFile != "" {
		if proxyTlsConf, err = parseMongoPemFile(pemFile, ctx.String("listen-tls-file-password")); err != nil {
			return err
		}
	}

	proxyConf, err := proxy.NewConfig(
		proxy.WithListenAddress(ctx.String("listen-address")),
		proxy.WithRequestHandler(reqHandler),
		proxy.WithTLS(proxyTlsConf),
		proxy.WithLogger(rootLogger.WithField("module", "proxy")),
	)
	if err != nil {
		return err
	}

	srvCtx := signalAwareContext(context.Background())
	return proxy.NewServer(proxyConf).Listen(srvCtx)
}

func parseMongoPemFile(pemFile, password string) (*tls.Config, error) {
	data, err := ioutil.ReadFile(pemFile)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: %w", pemFile, err)
	}

	var (
		nextBlock  *pem.Block
		serverCert []byte
		privateKey []byte
	)
	for {
		if nextBlock, data = pem.Decode(data); nextBlock == nil {
			break // No more PEM blocks available
		}

		if x509.IsEncryptedPEMBlock(nextBlock) {
			if password == "" {
				return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: contents are encrypted but no password was provided", pemFile)
			}

			if nextBlock.Bytes, err = x509.DecryptPEMBlock(nextBlock, []byte(password)); err != nil {
				return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: pem block decryption failed: %w", pemFile, err)
			}
		}

		switch nextBlock.Type {
		case "CERTIFICATE":
			serverCert = pem.EncodeToMemory(nextBlock)
		case "RSA PRIVATE KEY":
			privateKey = pem.EncodeToMemory(nextBlock)
		default:
			return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: unexpected pem block of type %q", pemFile, nextBlock.Type)
		}
	}

	if serverCert == nil {
		return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: missing server certificate", pemFile)
	} else if privateKey == nil {
		return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: missing private key", pemFile)
	}

	cert, err := tls.X509KeyPair(serverCert, privateKey)
	if err != nil {
		return nil, xerrors.Errorf("unable to parse mongo TLS cert from %s: unable to parse x509 keypair: %w", pemFile, err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}

func signalAwareContext(ctx context.Context) context.Context {
	wrappedCtx, cancelFn := context.WithCancel(ctx)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT)
		s := <-sigCh
		appLogger.WithField("signal", s.String()).Info("terminating due to signal")
		cancelFn()
	}()
	return wrappedCtx
}
