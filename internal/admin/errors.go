package admin

import "fmt"

type ErrorCode string

const (
	CodeInvalidConfiguration ErrorCode = "invalid_configuration"
	CodeRequestFailed        ErrorCode = "request_failed"
	CodeDecodeFailed         ErrorCode = "decode_failed"
	CodeSigningFailed        ErrorCode = "signing_failed"
)

type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func errorf(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
