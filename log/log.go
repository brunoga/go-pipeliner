package log

import (
	base_modules "github.com/brunoga/go-modules"
)

type LogEntry struct {
	module base_modules.Module
	err error
}

func NewLogEntry(module base_modules.Module, err error) *LogEntry {
	return &LogEntry{
		module,
		err,
	}
}

type Logger interface {
	SetLogChannel(chan<- *LogEntry)
}

