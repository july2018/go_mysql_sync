package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

// Level 日志级别
type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

var levelNames = map[Level]string{
	DebugLevel: "DEBUG",
	InfoLevel:  "INFO",
	WarnLevel:  "WARN",
	ErrorLevel: "ERROR",
}

// Logger 结构化日志器（兼容 Go 1.20，替代 slog）
type Logger struct {
	mu     sync.Mutex
	logger *log.Logger
	level  Level
	out    io.Writer
	fields []interface{}
}

// New 创建 Logger
func New(out io.Writer, level Level) *Logger {
	return &Logger{
		logger: log.New(out, "", 0),
		level:  level,
		out:    out,
	}
}

// With 添加结构化字段，返回子 logger
func (l *Logger) With(keyvals ...interface{}) *Logger {
	l.mu.Lock()
	defer l.mu.Unlock()
	fields := make([]interface{}, len(l.fields))
	copy(fields, l.fields)
	return &Logger{
		logger: l.logger,
		level:  l.level,
		out:    l.out,
		fields: append(fields, keyvals...),
	}
}

// logf 内部日志输出
func (l *Logger) logf(level Level, format string, args ...interface{}) {
	if level < l.level {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	prefix := levelNames[level]
	if len(l.fields) > 0 {
		prefix += " [" + formatFields(l.fields) + "]"
	}

	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("%s %s", prefix, msg)
}

// Debug 输出 DEBUG 级别日志
func (l *Logger) Debug(format string, args ...interface{}) {
	l.logf(DebugLevel, format, args...)
}

// Info 输出 INFO 级别日志
func (l *Logger) Info(format string, args ...interface{}) {
	l.logf(InfoLevel, format, args...)
}

// Warn 输出 WARN 级别日志
func (l *Logger) Warn(format string, args ...interface{}) {
	l.logf(WarnLevel, format, args...)
}

// Error 输出 ERROR 级别日志
func (l *Logger) Error(format string, args ...interface{}) {
	l.logf(ErrorLevel, format, args...)
}

// Fatal 输出 ERROR 级别日志并退出
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.logf(ErrorLevel, format, args...)
	os.Exit(1)
}

// formatFields 将 key-value 对格式化为字符串
func formatFields(fields []interface{}) string {
	parts := make([]string, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		parts = append(parts, fmt.Sprintf("%v=%v", fields[i], fields[i+1]))
	}
	return strings.Join(parts, " ")
}

// ParseLevel 从字符串解析日志级别
func ParseLevel(s string) Level {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return DebugLevel
	case "WARNING", "WARN":
		return WarnLevel
	case "ERROR":
		return ErrorLevel
	default:
		return InfoLevel
	}
}
