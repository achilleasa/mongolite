package emulator

import (
	"io"
	"io/ioutil"
	"strings"

	"github.com/achilleasa/mongolite/protocol"
	"golang.org/x/xerrors"
	"gopkg.in/Sirupsen/logrus.v1"
)

// Backend is implemented by types that can emulate mongo commands and
// generate suitable responses.
type Backend interface {
	// Name returns the name of the backend.
	Name() string

	// HandleRequest processes a decoded client request and returns back
	// a Response payload.
	HandleRequest(clientID string, req protocol.Request) (protocol.Response, error)

	// RemoveClient is invoked when a particular client disconnects and
	// allows the backend to perform any required state cleanup tasks.
	RemoveClient(clientID string) error
}

type cmdHandlerFn func(Backend, string, *protocol.CommandRequest) (protocol.Response, error)

// MongoEmulator emulates a mongo server by delegating CRUD requests to a
// pluggable backend and handling a subset of common mongo commands.
type MongoEmulator struct {
	b      Backend
	logger *logrus.Entry

	// A map which stores the last seen error for each clientID
	lastError map[string]error

	// A list of handlers for common mongo commands. The emulator will
	// try to use them when a request specifies a command that the backend
	// does not know how to handle. The map keys are stored uppercased so
	// we can handle commands in a case-insensitive manner.
	cmdHandlers map[string]cmdHandlerFn
}

// NewMongoEmulator returns a MongoEmulator instance that delegates CRUD
// operations to the provided Backend instance.
func NewMongoEmulator(b Backend, logger *logrus.Entry) (*MongoEmulator, error) {
	if b == nil {
		return nil, xerrors.Errorf("no backend specified")
	} else if logger == nil {
		// Use null-logger instead
		logger = logrus.NewEntry(&logrus.Logger{Out: ioutil.Discard})
	}

	emu := &MongoEmulator{
		b:         b,
		logger:    logger,
		lastError: make(map[string]error),
	}
	emu.registerCommandHandlers()
	return emu, nil
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
		if req.GetReplyType() == protocol.ReplyTypeNone {
			return nil
		}

		res = toErrorResponse(err, req.GetReplyType())
	}

	// Reset last error
	emu.lastError[clientID] = nil

	// Serialize response if this request expects one.
	if req.GetReplyType() != protocol.ReplyTypeNone {
		return protocol.Encode(w, res, req.RequestID(), req.GetReplyType())
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
	// Ask backend to process request.
	res, err := emu.b.HandleRequest(clientID, req)

	// The generic backend emulates some common mongo client commands.
	// Check if this one of them.
	if xerrors.Is(err, ErrUnsupportedRequest) {
		if req.GetType() == protocol.RequestTypeCommand {
			return emu.maybeProcessClientCommand(clientID, req.(*protocol.CommandRequest))
		}
	}

	return res, err
}

// maybeProcessClientCommand attempts to handle a mongo client command using one
// of the registered command handlers and returns ErrUnsupportedRequest if the
// command cannot be handled.
func (emu *MongoEmulator) maybeProcessClientCommand(clientID string, req *protocol.CommandRequest) (protocol.Response, error) {
	if h, found := emu.cmdHandlers[strings.ToUpper(req.Command)]; found {
		return h(emu.b, clientID, req)
	}

	emu.logger.WithFields(logrus.Fields{
		"client_id": clientID,
		"cmd":       req.Command,
	}).Warn("unsupported command")

	return protocol.Response{}, xerrors.Errorf("command %q: %w", req.Command, ErrUnsupportedRequest)
}
