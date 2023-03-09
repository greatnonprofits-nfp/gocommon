package dbutil

import (
	"errors"
	"fmt"

	"github.com/lib/pq"
)

// IsUniqueViolation returns true if the given error is a violation of unique constraint
func IsUniqueViolation(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code.Name() == "unique_violation"
	}
	return false
}

// QueryError is an error type for failed SQL queries
type QueryError struct {
	cause   error
	message string
	sql     string
	sqlArgs []interface{}
}

func (e *QueryError) Error() string {
	return e.message + ": " + e.cause.Error()
}

func (e *QueryError) Unwrap() error {
	return e.cause
}

func (e *QueryError) Query() (string, []interface{}) {
	return e.sql, e.sqlArgs
}

func NewQueryErrorf(cause error, sql string, sqlArgs []interface{}, message string, msgArgs ...interface{}) error {
	return &QueryError{
		cause:   cause,
		message: fmt.Sprintf(message, msgArgs...),
		sql:     sql,
		sqlArgs: sqlArgs,
	}
}

func AsQueryError(err error) *QueryError {
	var qerr *QueryError
	errors.As(err, &qerr)
	return qerr
}
