package tglog

import (
	"fmt"
	"os"

	"github.com/ipfs/go-log/v2"
	"github.com/testground/sdk-go/runtime"
)

type RunenvLogger struct {
	Re *runtime.RunEnv
}

func (r *RunenvLogger) Debug(args ...interface{}) {
	r.Re.RecordMessage("debug:", args...)
}

func (r *RunenvLogger) Debugf(format string, args ...interface{}) {
	r.Re.RecordMessage("debug:"+format, args...)
}

func (r *RunenvLogger) Error(args ...interface{}) {
	r.Re.RecordMessage("error:", args...)
}

func (r *RunenvLogger) Errorf(format string, args ...interface{}) {
	r.Re.RecordMessage("error:"+format, args...)
}

func (r *RunenvLogger) Fatal(args ...interface{}) {
	r.Re.RecordMessage("fatal:", args...)
	os.Exit(1)
}

func (r *RunenvLogger) Fatalf(format string, args ...interface{}) {
	r.Re.RecordMessage("fatal:"+format, args...)
	os.Exit(1)
}
func (r *RunenvLogger) Info(args ...interface{}) {
	r.Re.RecordMessage("info:", args...)
}

func (r *RunenvLogger) Infof(format string, args ...interface{}) {
	r.Re.RecordMessage("info:"+format, args...)
}

func (r *RunenvLogger) Panic(args ...interface{}) {
	msg := fmt.Sprintf("panic", args...)
	r.Re.RecordMessage("panic:", args...)
	panic(msg)
}

func (r *RunenvLogger) Panicf(format string, args ...interface{}) {
	msg := fmt.Sprintf("panic:"+format, args...)
	r.Re.RecordMessage("panic:"+format, args...)
	panic(msg)
}

func (r *RunenvLogger) Warn(args ...interface{}) {
	r.Re.RecordMessage("warn:", args...)
}

func (r *RunenvLogger) Warnf(format string, args ...interface{}) {
	r.Re.RecordMessage("warn:"+format, args...)
}

var _ log.StandardLogger = (*RunenvLogger)(nil)
