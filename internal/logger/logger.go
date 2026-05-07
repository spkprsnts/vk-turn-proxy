// Package logger provides a simple leveled logging interface shared across
// all packages in this module. Verbose/debug output is gated by SetVerbose.
package logger

import (
	"log"
	"sync/atomic"
)

var verboseEnabled atomic.Bool //nolint:gochecknoglobals

// SetVerbose enables or disables verbose/debug logging for all packages.
func SetVerbose(enabled bool) {
	verboseEnabled.Store(enabled)
}

// IsVerbose reports whether verbose logging is enabled.
func IsVerbose() bool {
	return verboseEnabled.Load()
}

// Infof logs a formatted informational message (always shown).
func Infof(format string, v ...any) {
	log.Printf(format, v...)
}

// Debugf logs a formatted message only when verbose mode is enabled.
func Debugf(format string, v ...any) {
	if verboseEnabled.Load() {
		log.Printf(format, v...)
	}
}

// Verbosef is an alias for Debugf.
func Verbosef(format string, v ...any) {
	if verboseEnabled.Load() {
		log.Printf(format, v...)
	}
}
