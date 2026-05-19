package logger

import (
	"testing"
)

func TestLoggerIsInitialized(t *testing.T) {
	if Log == nil {
		t.Fatal("Log is nil after init")
	}
}

func TestLoggerCanPrint(t *testing.T) {
	Log.Info("test message", "key", "value")
}
