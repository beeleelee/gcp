package logger

import (
	"log/slog"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var Log *slog.Logger

func init() {
	zl := log.Logger
	handler := zerolog.NewSlogHandler(zl)
	Log = slog.New(handler)
}
