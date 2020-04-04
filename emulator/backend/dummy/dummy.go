package dummy

import (
	"github.com/achilleasa/mongolite/emulator"
	"github.com/achilleasa/mongolite/protocol"
)

// Backend implements a dummy emulator backend that returns ErrUnsupportedRequest
// for all incoming requests.
type Backend struct{}

// NewDummyBackend returns a dummy backend instance.
func NewDummyBackend() *Backend {
	return new(Backend)
}

// Name returns the name of the backend.
func (b *Backend) Name() string { return "dummy" }

// HandleRequest processes a decoded client request and returns back
// a Response payload. The dummy payload always returns ErrUnsupportedRequest
func (b *Backend) HandleRequest(clientID string, req protocol.Request) (protocol.Response, error) {
	return protocol.Response{}, emulator.ErrUnsupportedRequest
}

// RemoveClient is invoked when a particular client disconnects and
// allows the backend to perform any required state cleanup tasks.
func (b *Backend) RemoveClient(clientID string) error { return nil }
