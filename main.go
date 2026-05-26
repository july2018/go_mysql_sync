package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go_mysql_sync/config"
	"go_mysql_sync/fullsync"
	"go_mysql_sync/incremental"

	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	mode := flag.String("mode", "all", "运行模式: full=仅全量, incremental=仅增量, all=全量后增量")
	resetPosition := flag.Bool("reset-position", false, "重置 Binlog 位点，从当前位置开始增量（谨慎使用）")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Println("go_mysql_sync v1.0.0")
		os.Exit(0)
	}

	// 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] %v\n", err)
		os.Exit(1)
	}

	// 初始化日志
	logger := setupLogging(cfg)

	logger.Info("============================================================")
	logger.Info("MySQL 同步程序启动 (Go版)")
	logger.Info("运行模式", "mode", *mode)
	logger.Info("源库", "host", cfg.Source.Host, "port", cfg.Source.PortOrDefault())
	logger.Info("目标库", "host", cfg.Target.Host, "port", cfg.Target.PortOrDefault())
	logger.Info("============================================================")

	// 信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("收到中断信号，程序退出")
		os.Exit(0)
	}()

	// 执行同步
	var fullErr, incErr error

	if *mode == "full" || *mode == "all" {
		if cfg.Sync.FullSync.Enabled {
			logger.Info("开始全量同步...")
			mgr := fullsync.New(cfg)
			fullErr = mgr.Run()
			if fullErr != nil {
				logger.Error("全量同步失败", "error", fullErr)
			} else {
				logger.Info("全量同步完成")
			}
		} else {
			logger.Info("全量同步已禁用，跳过")
		}
	}

	if (*mode == "incremental" || *mode == "all") && fullErr == nil {
		if cfg.Sync.Incremental.Enabled {
			logger.Info("开始增量同步（监听 Binlog）...")
			mgr, err := incremental.NewIncrementalSyncManager(cfg)
			if err != nil {
				logger.Error("创建增量同步管理器失败", "error", err)
				os.Exit(1)
			}
			defer mgr.Close()

			if *resetPosition {
				logger.Warn("--reset-position 已指定，将重置 Binlog 位点")
				mgr.ResetPosition()
			}

			incErr = mgr.Run()
			if incErr != nil {
				logger.Error("增量同步失败", "error", incErr)
				os.Exit(1)
			}
		} else {
			logger.Info("增量同步已禁用，跳过")
		}
	}

	if fullErr != nil {
		os.Exit(1)
	}
}

// setupLogging 初始化日志（同时输出到控制台和文件）
func setupLogging(cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Logging.Level {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	// 确保日志目录存在
	logDir := "logs"
	for i := len(cfg.Logging.File) - 1; i >= 0; i-- {
		if cfg.Logging.File[i] == '/' || cfg.Logging.File[i] == '\\' {
			logDir = cfg.Logging.File[:i]
			break
		}
	}
	os.MkdirAll(logDir, 0755)

	// 文件输出（滚动）
	fileWriter := &lumberjack.Logger{
		Filename:   cfg.Logging.File,
		MaxSize:    cfg.Logging.MaxBytes / 1024 / 1024,
		MaxBackups: cfg.Logging.BackupCount,
		MaxAge:     30,
		Compress:   true,
	}

	// 多输出：文件 + 控制台
	multiWriter := io.MultiWriter(os.Stdout, fileWriter)

	handler := slog.NewTextHandler(multiWriter, &slog.HandlerOptions{
		Level: level,
	})

	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
