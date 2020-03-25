package proxy

import "io"

// RequestHandler is implemented by objects that process incoming requests from
// mongo clients.
type RequestHandler interface {
	// HandleRequest processes the incoming request and writes a response
	// using the mongo wire format back to the provided io.Writer.
	HandleRequest(clientID string, w io.Writer, r []byte) error

	// RemoveClient is invoked when the remote mongo client disconnects.
	RemoveClient(clientID string) error
}
