package one

import "testing"

// A hand-written fixture: parsed by the register's check, never compiled (it sits under testdata).

func TestPlain(t *testing.T) {}

func TestUnnamedParameter(*testing.T) {}

// Not a test function: the wrong parameter list.
func TestWrongSignature(t *testing.T, n int) {}

// Not a test function: a method.
type sample struct{}

func (sample) TestMethod(t *testing.T) {}

// Not a test function: a helper that does not start with Test.
func notATest(t *testing.T) {}
