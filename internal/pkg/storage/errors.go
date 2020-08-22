package storage

import "github.com/pkg/errors"

var (
	ErrorNotFound = errors.New("not found")
	ErrorLocked   = errors.New("can't lock")
)
