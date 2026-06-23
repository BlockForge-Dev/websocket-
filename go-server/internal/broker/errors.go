package broker

import "errors"

// ErrBrokerClosed is returned when a closed broker is used.
var ErrBrokerClosed = errors.New("broker is closed")
