package emulator

import "golang.org/x/xerrors"

var (
	// ErrUnsupportedRequest is returned by backends when they cannot
	// process a particular mongo client request.
	ErrUnsupportedRequest = xerrors.New("unsupported request")

	// ErrInvalidCursor is returned by backends when a request includes an
	// unknown/invalid cursor ID.
	ErrInvalidCursor = xerrors.New("invalid cursor")
)
