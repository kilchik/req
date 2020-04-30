package req

import (
	"context"
	"log"
)

type Logger interface {
	Error(ctx context.Context, args ...interface{})
	Errorf(ctx context.Context, format string, args ...interface{})
	Info(ctx context.Context, args ...interface{})
	Infof(ctx context.Context, format string, args ...interface{})
	Warn(ctx context.Context, args ...interface{})
	Warnf(ctx context.Context, format string, args ...interface{})
	Debug(ctx context.Context, args ...interface{})
	Debugf(ctx context.Context, format string, args ...interface{})
}

type defaultLogger struct {}

func (l *defaultLogger) Error(ctx context.Context, args ...interface{}) {
	args = append([]interface{}{"[E] "}, args...)
	log.Print(args...)
}

func (l *defaultLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	format = "[E] " + format
	log.Printf(format, args...)
}

func (l *defaultLogger) Info(ctx context.Context, args ...interface{}) {
	args = append([]interface{}{"[I] "}, args...)
	log.Print(args...)
}

func (l *defaultLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	format = "[I] " + format
	log.Printf(format, args...)
}

func (l *defaultLogger) Warn(ctx context.Context, args ...interface{}) {
	args = append([]interface{}{"[W] "}, args...)
	log.Print(args...)
}

func (l *defaultLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	format = "[W] " + format
	log.Printf(format, args...)
}

func (l *defaultLogger) Debug(ctx context.Context, args ...interface{}) {
	args = append([]interface{}{"[D] "}, args...)
	log.Print(args...)
}

func (l *defaultLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	format = "[D] " + format
	log.Printf(format, args...)
}
