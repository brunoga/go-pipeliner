package log

import (
	base_modules "github.com/brunoga/go-modules"
)

type LogEntry struct {
	Module base_modules.Module
	Err error
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

