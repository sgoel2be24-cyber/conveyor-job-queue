package main

import "fmt"

func errNotImplemented(cmdName string) error {
	return fmt.Errorf("%s: not implemented yet — scaffolding phase", cmdName)
}
