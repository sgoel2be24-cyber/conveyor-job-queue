package main

import (
	"fmt"

	"connectrpc.com/connect"
)

func errNotImplemented(cmdName string) error {
	return fmt.Errorf("%s: not implemented yet — scaffolding phase", cmdName)
}

// connectRequest wraps a protobuf message for a Connect call.
func connectRequest[T any](msg *T) *connect.Request[T] {
	return connect.NewRequest(msg)
}
