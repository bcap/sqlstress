package log

import (
	"log"
	"os"
)

const (
	ErrorLevel = 1 << iota
	WarnLevel
	InfoLevel
	DebugLevel
)

var Level = WarnLevel

var (
	DebugLogger *log.Logger
	WarnLogger  *log.Logger
	InfoLogger  *log.Logger
	ErrorLogger *log.Logger
)

func init() {
	DebugLogger = log.New(os.Stderr, "DEBUG ", log.Ldate|log.Ltime)
	InfoLogger = log.New(os.Stderr, "INFO  ", log.Ldate|log.Ltime)
	WarnLogger = log.New(os.Stderr, "WARN  ", log.Ldate|log.Ltime)
	ErrorLogger = log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime)
}

func Debug(message string) {
	doLog(DebugLogger, DebugLevel, message)
}

func Info(message string) {
	doLog(InfoLogger, InfoLevel, message)
}

func Warn(message string) {
	doLog(WarnLogger, WarnLevel, message)
}

func Error(message string) {
	doLog(ErrorLogger, ErrorLevel, message)
}

func Debugf(format string, v ...any) {
	doLogf(DebugLogger, DebugLevel, format, v...)
}

func Infof(format string, v ...any) {
	doLogf(InfoLogger, InfoLevel, format, v...)
}

func Warnf(format string, v ...any) {
	doLogf(WarnLogger, WarnLevel, format, v...)
}

func Errorf(format string, v ...any) {
	doLogf(ErrorLogger, ErrorLevel, format, v...)
}

func doLog(logger *log.Logger, loggerLevel int, message string) {
	if loggerLevel <= Level {
		logger.Print(message)
	}
}

func doLogf(logger *log.Logger, loggerLevel int, format string, v ...any) {
	if loggerLevel <= Level {
		logger.Printf(format, v...)
	}
}
