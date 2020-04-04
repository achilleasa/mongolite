package emulator

import (
	"fmt"
	"strings"
	"time"

	"github.com/achilleasa/mongolite/protocol"
	"golang.org/x/xerrors"
	"gopkg.in/mgo.v2/bson"
)

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

func handleIsMaster(_ Backend, clientID string, _ *protocol.CommandRequest) (protocol.Response, error) {
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

func handleWhatsMyURI(_ Backend, clientID string, _ *protocol.CommandRequest) (protocol.Response, error) {
	return protocol.Response{
		Documents: []bson.M{{
			"ok":  1,
			"you": clientID,
		}},
	}, nil
}

func handleBuildInfo(Backend, string, *protocol.CommandRequest) (protocol.Response, error) {
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

func handleReplSetGetStatus(_ Backend, _ string, req *protocol.CommandRequest) (protocol.Response, error) {
	if req.Collection.Database != "admin" {
		return protocol.Response{}, protocol.ServerErrorf(protocol.CodeUnauthorized, "replSetGetStatus may only be run against the admin database.")
	}

	// Emulate server with no replicas.
	return protocol.Response{}, protocol.ServerErrorf(protocol.CodeNoReplicationEnabled, "not running with --replSet")
}

func handleGetLog(b Backend, _ string, _ *protocol.CommandRequest) (protocol.Response, error) {
	return protocol.Response{
		Documents: []bson.M{{
			"ok": 1,
			// Abuse logs command to display a banner to mongo shell ;-)
			"log": strings.Split(fmt.Sprintf(`
_  _ ____ _  _ ____ ____ _    _ ___ ____ 
|\/| |  | |\ | | __ |  | |    |  |  |___ 
|  | |__| | \| |__] |__| |___ |  |  |___ 

Greetings from your friendly neighborhood mongolite server.
Serving incoming client request using the %q backend.
`, b.Name()), "\n"),
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
