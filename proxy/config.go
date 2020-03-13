package proxy

import (
	"crypto/tls"
	"io/ioutil"

	"golang.org/x/xerrors"
	"gopkg.in/Sirupsen/logrus.v1"
)

// Config encapsulates the required configuration options for spinning up a
// mongo proxy instance.
type Config struct {
	// The address to listen for incoming client connections.
	listenAddr string

	// If provided, a TLS listener will be created using these settings.
	tlsConfig *tls.Config

	// A user-defined handler for incoming client requests.
	reqHandler RequestHandler

	// A logger to use; if not specified a null logger will be used instead.
	logger *logrus.Entry
}

// NewConfig creates a new proxy configuration and applies the provided options.
func NewConfig(opts ...ConfigOption) (*Config, error) {
	// Start with some sane defaults
	nullLogger := logrus.New()
	nullLogger.SetOutput(ioutil.Discard)

	var cfg = Config{
		listenAddr: ":37017",
		logger:     logrus.NewEntry(nullLogger),
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validate the configuration.
func (c *Config) validate() error {
	if c.listenAddr == "" {
		return xerrors.Errorf("proxy listen address not specified")
	} else if c.reqHandler == nil {
		return xerrors.Errorf("request handler not specified")
	}

	return nil
}

// ConfigOption applies a configuration setting to a Config instance.
type ConfigOption func(*Config) error

// WithListenAddress specifies the address where the proxy listens for incoming
// client connections.
func WithListenAddress(addr string) ConfigOption {
	return func(c *Config) error {
		c.listenAddr = addr
		return nil
	}
}

// WithRequestHandler specifies the handler that will be invoked to process
// incoming client requests.
func WithRequestHandler(handler RequestHandler) ConfigOption {
	return func(c *Config) error {
		c.reqHandler = handler
		return nil
	}
}

// WithTLS enables TLS mode for incoming client connections using the specified
// TLS configuration settings.
func WithTLS(tlsConfig *tls.Config) ConfigOption {
	return func(c *Config) error {
		c.tlsConfig = tlsConfig
		return nil
	}
}

// WithLogger configures the proxy server to use the specified logger.
func WithLogger(logger *logrus.Entry) ConfigOption {
	return func(c *Config) error {
		c.logger = logger
		return nil
	}
}
