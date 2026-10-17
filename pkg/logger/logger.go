package logger

import (
	"log/slog"
	"os"
)

var L *slog.Logger

func Init() {
	L = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(L)
}

func Info(msg string, args ...any)  { L.Info(msg, args...) }
func Error(msg string, args ...any) { L.Error(msg, args...) }
func Warn(msg string, args ...any)  { L.Warn(msg, args...) }
func Debug(msg string, args ...any) { L.Debug(msg, args...) }
