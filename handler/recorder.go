package handler

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"golang.org/x/xerrors"
)

// Recorder implements a handler that logs the raw binary payloads of incoming
// requests and outgoing responses.
type Recorder struct {
	mu        sync.Mutex
	reqStream io.Writer
	resStream io.Writer
	resBuf    bytes.Buffer

	wrappedHandler RequestHandler
}

// NewRecorder creates a handler that intercepts incoming requests and outgoing
// responses of an existing RequestHandler and writes them to the specified
// stream.
func NewRecorder(reqStream, resStream io.Writer, h RequestHandler) *Recorder {
	return &Recorder{
		reqStream:      reqStream,
		resStream:      resStream,
		wrappedHandler: h,
	}
}

// HandleRequest implements RequestHandler.
func (s *Recorder) HandleRequest(w io.Writer, r []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Save a copy of the incoming request
	rLen := int32(len(r))
	if err := binary.Write(s.reqStream, binary.LittleEndian, &rLen); err != nil {
		return xerrors.Errorf("recorder: unable to write length of recorded request: %w", err)
	}
	n, err := s.reqStream.Write(r)
	if err != nil {
		return xerrors.Errorf("recorder: unable to write recorded request: %w", err)
	} else if n != int(rLen) {
		return xerrors.Errorf("recorder: wrote partial recorded request: expected to write %d bytes; wrote %d", rLen, n)
	}

	// Pass the request to the wrapped handler and record the response
	s.resBuf.Reset()
	if err = s.wrappedHandler.HandleRequest(&s.resBuf, r); err != nil {
		return err
	}

	// Save a copy of the recorded response
	capturedRes := s.resBuf.Bytes()
	rLen = int32(len(capturedRes))
	if err := binary.Write(s.resStream, binary.LittleEndian, &rLen); err != nil {
		return xerrors.Errorf("recorder: unable to write length of recorded response: %w", err)
	}
	n, err = s.resStream.Write(capturedRes)
	if err != nil {
		return xerrors.Errorf("recorder: unable to write recorded response: %w", err)
	} else if n != int(rLen) {
		return xerrors.Errorf("recorder: wrote partial recorded response: expected to write %d bytes; wrote %d", rLen, n)
	}

	// Write recorded response to the upstream writer
	n, err = w.Write(capturedRes)
	if err != nil {
		return xerrors.Errorf("recorder: unable to write recorded response: %w", err)
	} else if n != int(rLen) {
		return xerrors.Errorf("recorder: wrote partial recorded response: expected to write %d bytes; wrote %d", rLen, n)
	}
	return nil
}
