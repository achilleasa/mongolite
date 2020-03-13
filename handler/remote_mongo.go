package handler

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"time"

	"golang.org/x/xerrors"
)

// RemoteMongo acts as a pipe that relays requests/responses between a
// connected client and a remote mongo server.
type RemoteMongo struct {
	remote    net.Conn
	resBuffer bytes.Buffer
}

// NewRemoteMongoHandler returns a request handler that connects to a mongod
// instance at remoteAddr and relays requests/responses. The handler will
// attempt to establish a TLS connection to the remote server if a non-nil
// tlsConfig argument is provided.
func NewRemoteMongoHandler(remoteAddr string, tlsConfig *tls.Config) (*RemoteMongo, error) {
	var (
		conn        net.Conn
		err         error
		dialTimeout = 5 * time.Second
	)

	if tlsConfig == nil {
		conn, err = net.DialTimeout("tcp", remoteAddr, dialTimeout)
	} else {
		conn, err = tls.DialWithDialer(
			&net.Dialer{Timeout: dialTimeout},
			"tcp", remoteAddr, tlsConfig,
		)
	}
	if err != nil {
		return nil, xerrors.Errorf("remote-mongo: %w", err)
	}

	return &RemoteMongo{remote: conn}, nil
}

// HandleRequest implements RequestHandler.
func (h *RemoteMongo) HandleRequest(w io.Writer, r []byte) error {
	// Send request
	n, err := h.remote.Write(r)
	if err != nil {
		return xerrors.Errorf("remote-mongo: unable to write incoming request to remote destination")
	}

	if exp := len(r); n != exp {
		return xerrors.Errorf("remote-mongo: wrote partial request to remote destination; attempted to write %d bytes; wrote %d", exp, n)
	}

	// Read response and pipe it to w
	if err := h.pipeRemoteResponse(w); err != nil {
		return xerrors.Errorf("remote-mongo: unable to process remote response: %w", err)
	}

	return nil
}

func (h *RemoteMongo) pipeRemoteResponse(w io.Writer) error {
	h.resBuffer.Reset()

	// Wait for remote response
	n, err := io.CopyN(&h.resBuffer, h.remote, 16)
	if err != nil {
		return xerrors.Errorf("unable to read response header: %w", err)
	} else if n != 16 {
		return xerrors.Errorf("incomplete response header: expected 16 bytes; got %d", n)
	}

	// Decode and verify request length
	resLen := binary.LittleEndian.Uint32(h.resBuffer.Bytes())
	if resLen < 16 {
		return xerrors.Errorf("response header specifies invalid message length %d", resLen)
	}

	// Buffer remainder of request
	remaining := resLen - 16
	n, err = io.CopyN(&h.resBuffer, h.remote, int64(remaining))
	if err != nil {
		return xerrors.Errorf("unable to read remainder of response payload: %w", err)
	} else if n != int64(remaining) {
		return xerrors.Errorf("incomplete response payload: expected %d bytes; got %d", remaining, n)
	}

	// Write captured response to the provided writer
	n, err = h.resBuffer.WriteTo(w)
	if err != nil {
		return xerrors.Errorf("unable to write response payload to connected client: %w", err)
	} else if n != int64(resLen) {
		return xerrors.Errorf("wrote partial response payload to connected client: expected to write %d bytes; wrote %d", resLen, n)
	}

	return nil
}
