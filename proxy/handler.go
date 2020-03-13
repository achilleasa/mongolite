package proxy

import "io"

// RequestHandler is implemented by objects that process incoming requests from
// mongo clients.
type RequestHandler interface {
	// HandleRequest processes the incoming request and writes a response
	// using the mongo wire format back to the provided io.Writer.
	HandleRequest(io.Writer, []byte) error
}

// RequestHandlerFunc is an adaptor for converting a function with a suitable
// signature into a RequestHandler.
type RequestHandlerFunc func(io.Writer, []byte) error

// HandleRequest invokes the wrapped function with the provided arguments.
func (f RequestHandlerFunc) HandleRequest(w io.Writer, r []byte) error {
	return f(w, r)
}
