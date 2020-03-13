package proxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"

	"golang.org/x/xerrors"
	"gopkg.in/Sirupsen/logrus.v1"
)

// Server implements a proxy server that buffers incoming mongo requests and
// passes them to a user-defined handler for further processing.
type Server struct {
	cfg *Config
}

// NewServer creates a new proxy server instance using the specified config.
func NewServer(cfg *Config) *Server {
	return &Server{
		cfg: cfg,
	}
}

// Listen for incoming connections until ctx expires.
func (s *Server) Listen(ctx context.Context) error {
	l, err := s.createListener()
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := l.Accept()
		if err != nil {
			s.cfg.logger.WithError(err).Errorf("unable to accept incoming connection")
			break
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			logger := s.cfg.logger.WithField("client", conn.RemoteAddr().String())
			logger.Info("connection established")
			if err := s.handleConn(conn); err != nil {
				if xerrors.Is(err, io.EOF) {
					logger.Info("client disconnected; EOF")
					return
				}
				logger.WithError(err).Error("terminating connection")
			}
		}()
	}

	wg.Wait()
	s.cfg.logger.Info("shutting down")
	return nil
}

func (s *Server) createListener() (net.Listener, error) {
	var (
		l      net.Listener
		useTLS bool
		err    error
	)

	if s.cfg.tlsConfig == nil {
		l, err = net.Listen("tcp", s.cfg.listenAddr)
	} else {
		useTLS = true
		l, err = tls.Listen("tcp", s.cfg.listenAddr, s.cfg.tlsConfig)
	}

	if err != nil {
		return nil, err
	}

	s.cfg.logger.WithFields(logrus.Fields{
		"listen_at": s.cfg.listenAddr,
		"use_tls":   useTLS,
	}).Info("listening for incoming proxy connections")
	return l, nil
}

func (s *Server) handleConn(conn net.Conn) error {
	defer func() { _ = conn.Close() }()

	var reqBuffer bytes.Buffer
	for {
		if err := bufferNextRequest(conn, &reqBuffer); err != nil {
			return err
		}

		if err := s.cfg.reqHandler.HandleRequest(conn, reqBuffer.Bytes()); err != nil {
			return err
		}
	}
}

func bufferNextRequest(r io.Reader, b *bytes.Buffer) error {
	// Read mongo request header
	b.Reset()
	n, err := io.CopyN(b, r, 16)
	if err != nil {
		return xerrors.Errorf("unable to read next request header: %w", err)
	} else if n != 16 {
		return xerrors.Errorf("incomplete next request header: expected 16 bytes; got %d", n)
	}

	// Decode and verify request length
	reqLen := binary.LittleEndian.Uint32(b.Bytes())
	if reqLen < 16 {
		return xerrors.Errorf("request header specifies invalid message length %d", reqLen)
	}

	// Buffer remainder of request
	remaining := reqLen - 16
	n, err = io.CopyN(b, r, int64(remaining))
	if err != nil {
		return xerrors.Errorf("unable to read remainder of request payload: %w", err)
	} else if n != int64(remaining) {
		return xerrors.Errorf("incomplete next request payload: expected %d bytes; got %d", remaining, n)
	}

	return nil
}
