package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/achilleasa/mongolite/proxy"
	"github.com/achilleasa/mongolite/proxy/handler"
	"golang.org/x/xerrors"
	"gopkg.in/urfave/cli.v2"
)

// ProxyToRemote implements the proxy tool CLI command.
func ProxyToRemote(ctx *cli.Context) error {
	mongoHandler, err := makeRemoteMongoHandler(ctx)
	if err != nil {
		return err
	}

	recReqFile := ctx.String("rec-requests-to")
	recResFile := ctx.String("rec-responses-to")
	if recReqFile != "" || recResFile != "" {
		var reqStream, resStream = ioutil.Discard, ioutil.Discard
		if recReqFile != "" {
			f, err := os.Create(recReqFile)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			reqStream = f

			appLogger.WithField("to", recReqFile).Info("recording client requests")
		}
		if recResFile != "" {
			f, err := os.Create(recResFile)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			resStream = f

			appLogger.WithField("to", recResFile).Info("recording server responses")
		}

		// Wrap mongo proxy handler with a stream recorder.
		mongoHandler = handler.NewRecorder(reqStream, resStream, mongoHandler)
	}

	return startProxy(ctx, mongoHandler)
}

func makeRemoteMongoHandler(ctx *cli.Context) (proxy.RequestHandler, error) {
	var (
		remoteTLSConf *tls.Config
		err           error
	)
	if pemFile := ctx.String("remote-tls-file"); pemFile != "" {
		if remoteTLSConf, err = parseMongoPemFile(pemFile, ctx.String("remote-tls-file-password")); err != nil {
			return nil, err
		}

		if ctx.Bool("remote-tls-no-verify") {
			appLogger.Warn("disabling TLS verification when connecting to remote mongod")
			remoteTLSConf.InsecureSkipVerify = true
		}
	}

	return handler.NewRemoteMongoHandler(
		ctx.String("remote-address"),
		remoteTLSConf,
	)
}

func startProxy(ctx *cli.Context, reqHandler proxy.RequestHandler) error {
	var (
		proxyTLSConf *tls.Config
		err          error
	)

	if pemFile := ctx.String("listen-tls-file"); pemFile != "" {
		if proxyTLSConf, err = parseMongoPemFile(pemFile, ctx.String("listen-tls-file-password")); err != nil {
			return err
		}
	}

	proxyConf, err := proxy.NewConfig(
		proxy.WithListenAddress(ctx.String("listen-address")),
		proxy.WithRequestHandler(reqHandler),
		proxy.WithTLS(proxyTLSConf),
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
