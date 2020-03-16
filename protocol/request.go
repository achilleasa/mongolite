package protocol

import (
	"fmt"
	"sort"

	"gopkg.in/mgo.v2/bson"
)

// RequestType describes the type of a client request.
type RequestType string

// The supported request types.
const (
	RequestTypeUpdate        RequestType = "update"
	RequestTypeInsert        RequestType = "insert"
	RequestTypeGetMore       RequestType = "getMore"
	RequestTypeDelete        RequestType = "delete"
	RequestTypeKillCursors   RequestType = "killCursors"
	RequestTypeQuery         RequestType = "query"
	RequestTypeCommand       RequestType = "command"
	RequestTypeFindAndUpdate RequestType = "findAndUpdate"
	RequestTypeFindAndDelete RequestType = "findAndDelete"
	RequestTypeUnknown       RequestType = "unknown"
)

// AllRequestTypeNames returns a lexicographically sorted list with all
// request types supported by the decoder.
func AllRequestTypeNames() []string {
	list := []string{
		string(RequestTypeUpdate),
		string(RequestTypeInsert),
		string(RequestTypeGetMore),
		string(RequestTypeDelete),
		string(RequestTypeKillCursors),
		string(RequestTypeQuery),
		string(RequestTypeCommand),
		string(RequestTypeFindAndUpdate),
		string(RequestTypeFindAndDelete),
		string(RequestTypeUnknown),
	}
	sort.Strings(list)
	return list
}

// Request represents a client request.
type Request interface {
	// A private interface that is only implemented by types exported from
	// this package.
	isRequest()

	// Opcode returns the opcode identifying this request type.
	Opcode() int32

	// Type returns a string representation of this request type.
	Type() RequestType

	// ReplyExpected returns true if the server is expected to send back
	// a reply for this request.
	ReplyExpected() bool

	// RequestID returns the unique request ID for an incoming request.
	RequestID() int32
}

// header provides information about a request or response payload.
type header struct {
	messageLength int32
	requestID     int32
	responseTo    int32
	opcode        int32
}

// The size of the mongo header in bytes
const sizeOfHeader = 16

// payloadLength returns the size of the request payload exluding the header.
func (h header) payloadLength() int {
	return int(h.messageLength) - sizeOfHeader
}

// requestBase can be embedded as a mixin to a struct so as to ensure that it
// implements the Request interface.
type requestBase struct {
	h             header
	reqType       RequestType
	replyExpected bool
}

func (r requestBase) isRequest()          {}
func (r requestBase) Opcode() int32       { return r.h.opcode }
func (r requestBase) RequestID() int32    { return r.h.requestID }
func (r requestBase) Type() RequestType   { return r.reqType }
func (r requestBase) ReplyExpected() bool { return r.replyExpected }

// NamespacedCollection encodes a namespaced collection.
type NamespacedCollection struct {
	Database   string
	Collection string
}

// String implements fmt.Stringer for NamespacedCollection.
func (c NamespacedCollection) String() string { return fmt.Sprintf("%s.%s", c.Database, c.Collection) }

// UpdateFlag represents the allowed flag values for an update request.
type UpdateFlag uint32

const (
	// If set, the database will insert the supplied object into the
	// collection if no matching document is found.
	UpdateFlagUpsert UpdateFlag = 1 << iota

	// If set, the database will update all matching objects in the
	// collection. Otherwise only updates first matching document.
	UpdateFlagMulti
)

// UpdateRequest represents an update request.
type UpdateRequest struct {
	requestBase

	Collection NamespacedCollection
	Updates    []UpdateTarget
}

// UpdateTarget represents a single update operation.
type UpdateTarget struct {
	Selector     bson.M
	Update       bson.M
	ArrayFilters []bson.M
	Flags        UpdateFlag
}

// InsertFlag represents the allowed flag values for an insert request.
type InsertFlag uint32

const (
	// If set, the database will continue processing a bulk inseert request
	// even if an error occurs.
	InsertFlagContinueOnError InsertFlag = 1 << iota
)

// InsertRequest represents an single or bulk document insert request.
type InsertRequest struct {
	requestBase

	Collection NamespacedCollection
	Flags      InsertFlag
	Inserts    []bson.M
}

// GetMoreRequest represents a request to read additional documents off a cursor.
type GetMoreRequest struct {
	requestBase

	Collection  NamespacedCollection
	NumToReturn int32
	CursorID    int64
}

// ReplyExpected always returns true for GetMore requests.
func (GetMoreRequest) ReplyExpected() bool { return true }

// DeleteRequest represents a request to delete a set of documents.
type DeleteRequest struct {
	requestBase

	Collection NamespacedCollection
	Deletes    []DeleteTarget
}

// DeleteTarget represents a single delete operation.
type DeleteTarget struct {
	Selector bson.M
	Limit    int
}

// KillCursorsRequest represents a request to close a set of active cursors.
type KillCursorsRequest struct {
	requestBase

	CursorIDs []int64
}

// QueryFlag represents the allowed flag values for a query request.
type QueryFlag uint32

const (
	_ QueryFlag = 1 << iota // bit 0 is reserved.
	// Tailable means cursor is not closed when the last data is retrieved. Rather,
	// the cursor marks the final object’s position. You can resume using the
	// cursor later, from where it was located, if more data were received. Like
	// any “latent cursor”, the cursor may become invalid at some point
	// (CursorNotFound) – for example if the final object it references were
	// deleted.
	QueryFlagTailableCursor
	// Allow query of replica slave. Normally these return an error except for namespace “local”.
	QueryFlagSlaveOK
	// Internal replication use only - driver should not set.
	QueryFlagOplogReplay
	// The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use. Set this option to prevent that.
	QueryFlagNoCursorTimeout
	// Use with TailableCursor. If we are at the end of the data, block for a while rather than returning no data. After a timeout period, we do return as normal.
	QueryFlagAwaitData
	// Stream the data down full blast in multiple “more” packages, on the assumption that the client will fully read all data queried. Faster when you are pulling a lot of data and know you want to pull it all down. Note: the client is not allowed to not read all the data unless it closes the connection.
	QueryFlagExhaust
	// Get partial results from a mongos if some shards are down (instead of throwing an error)
	QueryFlagPartial
)

// QueryRequest represents a search query.
type QueryRequest struct {
	requestBase

	Collection    NamespacedCollection
	Flags         QueryFlag
	NumToSkip     int32
	NumToReturn   int32
	Query         bson.M
	Sort          bson.M
	FieldSelector bson.M
}

// FindAndUpdateRequest encapsulates the arguments for a find and replace
// command. This command updates the matched document and returns back
// either the original document or the modified document depending on the
// value of the ReturnUpdatedDoc flag.
//
// See https://docs.mongodb.com/manual/reference/command/findAndModify/#findandmodify
type FindAndUpdateRequest struct {
	requestBase

	Collection NamespacedCollection

	// Query for matching the document to update
	Query bson.M

	// Optional sort order in case multiple documents match the query. Only
	// the first document will be affected by this operation.
	Sort bson.M

	Update       bson.M
	ArrayFilters []bson.M

	// Create the document if missing.
	Upsert bool

	// If true, return back the updated document; otherwise return the
	// original document before applying the update.
	ReturnUpdatedDoc bool

	// An optional selector for the fields in the returned document.
	FieldSelector bson.M
}

// FindAndDeleteRequest encapsulates the arguments for a find and delete
// command (issued via a call to findAndModify with remove: true). This command
// deletes the matched document and returns it back to the caller.
//
// See https://docs.mongodb.com/manual/reference/command/findAndModify/#findandmodify
type FindAndDeleteRequest struct {
	requestBase

	Collection NamespacedCollection

	// Query for matching the document to update
	Query bson.M

	// Optional sort order in case multiple documents match the query. Only
	// the first document will be affected by this operation.
	Sort bson.M

	// An optional selector for the fields in the returned document.
	FieldSelector bson.M
}

// CommandRequest represents a mongo command sent by a mongo client.
type CommandRequest struct {
	requestBase

	Collection NamespacedCollection
	Command    string
	Args       bson.M
}

// UnknownRequest represents a client request that the parser does not know how
// to decode.
type UnknownRequest struct {
	requestBase

	// The raw payload of the captured request (sans header)
	Payload []byte
}
