package protocol

import "fmt"

// ErrorCode describes the type of error messages returned by a mongo server.
type ErrorCode int

// A common subset of the error codes returned by mongo servers. The full list
// can be found here:
// https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml.
const (
	CodeUnauthorized         ErrorCode = 13
	CodeNoReplicationEnabled ErrorCode = 76
)

func (ec ErrorCode) String() string {
	switch ec {
	case CodeUnauthorized:
		return "Unauthorized"
	case CodeNoReplicationEnabled:
		return "NoReplicationEnabled"
	default:
		return "Unknown"
	}
}

// ServerError describes a server error with an associated status code.
type ServerError struct {
	Msg  string
	Code ErrorCode
}

// ServerErrorf creates a formatted ServerError.
func ServerErrorf(code ErrorCode, format string, args ...interface{}) ServerError {
	return ServerError{
		Msg:  fmt.Sprintf(format, args...),
		Code: code,
	}
}

// Error returns a string representation for this error
func (e ServerError) Error() string {
	return fmt.Sprintf("%s (code %d): %s", e.Code.String(), e.Code, e.Msg)
}
