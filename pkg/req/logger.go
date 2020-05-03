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

type defaultLogger struct {
	disabled bool
}

func (l *defaultLogger) Error(ctx context.Context, args ...interface{}) {
	l.printArgs(ctx, "[E]", args...)
}

func (l *defaultLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	l.printFormatAndArgs(ctx, "[E]", format, args...)
}

func (l *defaultLogger) Info(ctx context.Context, args ...interface{}) {
	l.printArgs(ctx, "[I]", args...)
}

func (l *defaultLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	l.printFormatAndArgs(ctx, "[I]", format, args...)
}

func (l *defaultLogger) Warn(ctx context.Context, args ...interface{}) {
	l.printArgs(ctx, "[W]", args...)
}

func (l *defaultLogger) Warnf(ctx context.Context, format string, args ...interface{}) {
	l.printFormatAndArgs(ctx, "[W]", format, args...)
}

func (l *defaultLogger) Debug(ctx context.Context, args ...interface{}) {
	l.printArgs(ctx, "[D]", args...)
}

func (l *defaultLogger) Debugf(ctx context.Context, format string, args ...interface{}) {
	l.printFormatAndArgs(ctx, "[D]", format, args...)
}

func (l *defaultLogger) printArgs(ctx context.Context, prefix string, args ...interface{}) {
	if l.disabled {
		return
	}
	args = append([]interface{}{prefix + " "}, args...)
	log.Print(args...)
}

func (l *defaultLogger) printFormatAndArgs(ctx context.Context, prefix, format string, args ...interface{}) {
	if l.disabled {
		return
	}
	format = prefix + " " + format
	log.Printf(format, args...)
}
