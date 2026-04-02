package authn

import "fmt"

type ErrorKind string

const (
	ErrorKindMissingHeaders    ErrorKind = "missing_headers"
	ErrorKindUnsupportedSign   ErrorKind = "unsupported_sign_description"
	ErrorKindBadHeaders        ErrorKind = "bad_headers"
	ErrorKindBadClock          ErrorKind = "bad_clock"
	ErrorKindRequestorNotFound ErrorKind = "requestor_not_found"
	ErrorKindBadSignature      ErrorKind = "bad_signature"
	ErrorKindKeyStoreFailure   ErrorKind = "key_store_failure"
)

type Error struct {
	Kind    ErrorKind
	Message string
	Headers []string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}

	if len(e.Headers) == 0 {
		return fmt.Sprintf("%s: %s", e.Kind, e.Message)
	}

	return fmt.Sprintf("%s: %s (%v)", e.Kind, e.Message, e.Headers)
}

func (e *Error) HTTPStatus() int {
	if e == nil {
		return 0
	}

	switch e.Kind {
	case ErrorKindMissingHeaders, ErrorKindUnsupportedSign:
		return 400
	case ErrorKindBadHeaders, ErrorKindBadClock, ErrorKindRequestorNotFound, ErrorKindBadSignature:
		return 401
	default:
		return 500
	}
}

func newError(kind ErrorKind, message string, headers ...string) *Error {
	return &Error{
		Kind:    kind,
		Message: message,
		Headers: append([]string(nil), headers...),
	}
}
