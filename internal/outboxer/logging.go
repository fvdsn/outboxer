package outboxer

import (
	"log/slog"
	"os"
	"strings"
)

// setupLogging configures the default slog logger from the given level and
// format. Level is one of debug, info, warn, error. Format is text (the default,
// human readable) or json. Unknown values fall back to info and text.
func setupLogging(level string, format string) {
	options := &slog.HandlerOptions{Level: parseLogLevel(level)}

	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(os.Stdout, options)
	} else {
		handler = slog.NewTextHandler(os.Stdout, options)
	}

	slog.SetDefault(slog.New(handler))
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
