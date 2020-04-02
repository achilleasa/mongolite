package handler

import (
	"io"
	"strings"
	"time"

	"github.com/achilleasa/mongolite/protocol"
	"golang.org/x/xerrors"
	"gopkg.in/Sirupsen/logrus.v1"
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

type cmdHandlerFn func(string, *protocol.CommandRequest) (protocol.Response, error)

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
func NewMongoEmulator(b Backend, logger *logrus.Entry) *MongoEmulator {
	emu := &MongoEmulator{
		b:         b,
		logger:    logger,
		lastError: make(map[string]error),
	}
	emu.registerCommandHandlers()
	return emu
}

func (emu *MongoEmulator) registerCommandHandlers() {
	allCmds := map[string]cmdHandlerFn{
		"isMaster":         handleIsMaster,
		"whatsMyUri":       handleWhatsMyURI,
		"buildInfo":        handleBuildInfo,
		"replSetGetStatus": handleReplSetGetStatus,
		"getLog":           handleGetLog,
	}

	// Store command keys uppercased so we can perform case-insensitive lookups.
	emu.cmdHandlers = make(map[string]cmdHandlerFn, len(allCmds))
	for cmdName, cmdFn := range allCmds {
		emu.cmdHandlers[strings.ToUpper(cmdName)] = cmdFn
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
		if req.ReplyType() == protocol.ReplyTypeNone {
			return nil
		}

		res = toErrorResponse(err, req.ReplyType())
	}

	// Reset last error
	emu.lastError[clientID] = nil

	// Serialize response if this request expects one.
	if req.ReplyType() != protocol.ReplyTypeNone {
		return protocol.Encode(w, res, req.RequestID(), req.ReplyType())
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
		return h(clientID, req)
	}

	emu.logger.WithFields(logrus.Fields{
		"client_id": clientID,
		"cmd":       req.Command,
	}).Warn("unsupported command")

	return protocol.Response{}, xerrors.Errorf("command %q: %w", req.Command, ErrUnsupportedRequest)
}

func handleIsMaster(clientID string, _ *protocol.CommandRequest) (protocol.Response, error) {
	return protocol.Response{
		Documents: []bson.M{{
			"ok":                  1,
			"ismaster":            true,
			"secondary":           false,
			"readOnly":            false,
			"maxBsonObjectSize":   16 * 1024 * 1024,
			"maxMessageSizeBytes": 48 * 1000 * 1000,
			"maxWriteBatchSize":   10000,
			"localTime":           time.Now().UTC(),
			"connectionId":        clientID,
			"minWireVersion":      1,
			"maxWireVersion":      6,
		}},
	}, nil
}

func handleWhatsMyURI(clientID string, _ *protocol.CommandRequest) (protocol.Response, error) {
	return protocol.Response{
		Documents: []bson.M{{
			"ok":  1,
			"you": clientID,
		}},
	}, nil
}

func handleBuildInfo(string, *protocol.CommandRequest) (protocol.Response, error) {
	return protocol.Response{
		Documents: []bson.M{{
			"ok": 1,
			// We are emulating mongod 3.6.8
			"version":           "3.6.8",
			"versionArray":      []int{3, 6, 8, 0},
			"maxBsonObjectSize": 16 * 1024 * 1024,
		}},
	}, nil
}

func handleReplSetGetStatus(_ string, req *protocol.CommandRequest) (protocol.Response, error) {
	if req.Collection.Database != "admin" {
		return protocol.Response{}, protocol.ServerErrorf(protocol.CodeUnauthorized, "replSetGetStatus may only be run against the admin database.")
	}

	// Emulate server with no replicas.
	return protocol.Response{}, protocol.ServerErrorf(protocol.CodeNoReplicationEnabled, "not running with --replSet")
}

func handleGetLog(string, *protocol.CommandRequest) (protocol.Response, error) {
	return protocol.Response{
		Documents: []bson.M{{
			"ok": 1,
			// Abuse logs command to display a banner to mongo shell ;-)
			"log": strings.Split(`
_  _ ____ _  _ ____ ____ _    _ ___ ____ 
|\/| |  | |\ | | __ |  | |    |  |  |___ 
|  | |__| | \| |__] |__| |___ |  |  |___ 

Greetings from your friendly neigborhood mongolite server
WARNING: only a subset of mongo commands are working
`, "\n"),
		}},
	}, nil
}

// toErrorResponse converts a standard error into a mongo response payload.
func toErrorResponse(err error, replyType protocol.ReplyType) protocol.Response {
	var flags protocol.ResponseFlag
	if xerrors.Is(err, ErrInvalidCursor) {
		flags |= protocol.ResponseFlagCursorNotFound
	} else {
		flags |= protocol.ResponseFlagQueryError
	}

	var errDoc bson.M
	if replyType == protocol.ReplyTypeOpReply {
		errDoc = bson.M{"$err": err.Error()}
	} else {
		errDoc = bson.M{"errmsg": err.Error()}

		// Server errors contain additional information.
		if srvErr, ok := err.(protocol.ServerError); ok {
			errDoc["errmsg"] = srvErr.Msg
			errDoc["code"] = srvErr.Code
			errDoc["codeName"] = srvErr.Code.String()
		}
	}

	errDoc["ok"] = 0

	return protocol.Response{
		Flags:     flags,
		Documents: []bson.M{errDoc},
	}
}
