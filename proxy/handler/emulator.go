package handler

import (
	"io"

	"github.com/achilleasa/mongolite/protocol"
	"golang.org/x/xerrors"
	"gopkg.in/mgo.v2/bson"
)

var (
	// ErrErrUnsupportedRequest is returned by backends when they cannot
	// process a particular mongo client request.
	ErrUnsupportedRequest = xerrors.New("unsupported request")

	// ErrInvalidCursor is returned by backends when a request includes an
	// unknown/invalid cursor ID.
	ErrInvalidCursor = xerrors.New("invalid cursor")
)

// Backend is implemented by types that can emulate mongo commands and generate
// suitable responses.
type Backend interface {
	// HandleRequest processes a decoded client request and returns back
	// a Response payload.
	HandleRequest(clientID string, req protocol.Request) (protocol.Response, error)

	// RemoveClient is invoked when a particular client disconnects and
	// allows the backend to perform any required state cleanup tasks.
	RemoveClient(clientID string) error
}

var (
	// A list of handlers for common mongo commands. The emulator will
	// try to use them when a request specifies a command that the backend
	// does not know how to handle.
	cmdHandlers = map[string]func(string, *protocol.CommandRequest) (protocol.Response, error){}
)

// MongoEmulator emulates a mongo server by delegating CRUD requests to a
// pluggable backend and handling a subset of common mongo commands.
type MongoEmulator struct {
	b Backend

	// A map which stores the last seen error for each clientID
	lastError map[string]error
}

// NewMongoEmulator returns a MongoEmulator instance that delegates CRUD
// operations to the provided Backend instance.
func NewMongoEmulator(b Backend) *MongoEmulator {
	return &MongoEmulator{
		b:         b,
		lastError: make(map[string]error),
	}
}

// HandleRequest implements the RequestHandler interface. This method processes
// an incoming mongo request by first dispatching it to the configured backend.
// If the backend is unable to handle the request, the method checks whether
// the request includes one of the generic mongo commands supported by the
// emulator and handles it instead.
//
// If an error occurs while handling a request, the emulator will first check
// whether the request expects a response and if so, serialize the error and
// write it back to the response stream. Otherwise, the error is buffered
// and can be retrieved by the client via a getLastError command.
func (emu *MongoEmulator) HandleRequest(clientID string, w io.Writer, reqData []byte) error {
	req, err := protocol.Decode(reqData)
	if err != nil {
		return xerrors.Errorf("unable to decode incoming request: %w", err)
	}

	res, err := emu.process(clientID, req)
	if err != nil {
		emu.lastError[clientID] = err
		if !req.ReplyExpected() {
			return nil
		}

		res = toErrorResponse(err)
	}

	// Reset last error
	emu.lastError[clientID] = nil

	// Serialize response if this request expects one.
	if req.ReplyExpected() {
		return protocol.Encode(w, res, req.RequestID())
	}
	return nil
}

// RemoveClient implements RequestHandler. It makes sure that any
// client-specific state tracked by the emulator or its backend is properly
// cleaned up when the remote client disconnects.
func (emu *MongoEmulator) RemoveClient(clientID string) error {
	delete(emu.lastError, clientID)
	if emu.b == nil {
		return nil
	}
	return emu.b.RemoveClient(clientID)
}

func (emu *MongoEmulator) process(clientID string, req protocol.Request) (protocol.Response, error) {
	var (
		res protocol.Response
		err = ErrUnsupportedRequest
	)

	if emu.b != nil {
		res, err = emu.b.HandleRequest(clientID, req)
	}

	// The generic backend emulates some common mongo client commands.
	// Check if this one of them.
	if xerrors.Is(err, ErrUnsupportedRequest) {
		if req.Type() == protocol.RequestTypeCommand {
			return maybeProcessClientCommand(clientID, req.(*protocol.CommandRequest))
		}
	}

	return res, err
}

// toErrorResponse converts a standard error into a mongo response payload.
func toErrorResponse(err error) protocol.Response {
	var flags protocol.ResponseFlag
	if xerrors.Is(err, ErrInvalidCursor) {
		flags |= protocol.ResponseFlagCursorNotFound
	} else {
		flags |= protocol.ResponseFlagQueryError
	}

	return protocol.Response{
		Flags: flags,
		Documents: []bson.M{
			{"$err": err.Error()},
		},
	}
}

// maybeProcessClientCommand attempts to handle a mongo client command using one
// of the registered command handlers and returns ErrUnsupportedRequest if the
// command cannot be handled.
func maybeProcessClientCommand(clientID string, req *protocol.CommandRequest) (protocol.Response, error) {
	if h, found := cmdHandlers[req.Command]; found {
		return h(clientID, req)
	}

	return protocol.Response{}, xerrors.Errorf("command %q: %w", req.Command, ErrUnsupportedRequest)
}
