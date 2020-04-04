package protocol

import (
	"gopkg.in/mgo.v2/bson"
)

// ResponseFlag represents the allowed flag values for a reply message.
type ResponseFlag uint32

// The list of supported response flags.
const (
	// Set when getMore is called but the cursor id is not valid at the
	// server.
	ResponseFlagCursorNotFound ResponseFlag = 1 << iota

	// Set when a query failed. The reply will include a single document
	// with more details about the error.
	ResponseFlagQueryError
)

// Response represents a response to a mongo client request.
type Response struct {
	Flags        ResponseFlag
	CursorID     int64
	StartingFrom int32
	Documents    []bson.M
}
