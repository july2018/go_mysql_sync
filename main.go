package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go_mysql_sync/config"
	"go_mysql_sync/fullsync"
	"go_mysql_sync/incremental"
	"go_mysql_sync/logger"

	"gopkg.in/natefinch/lumberjack.v2"
)

var log *logger.Logger

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
	log = setupLogging(cfg)

	log.Info("============================================================")
	log.Info("MySQL 同步程序启动 (Go版)")
	log.Info("运行模式: %s", *mode)
	log.Info("源库: %s:%d", cfg.Source.Host, cfg.Source.PortOrDefault())
	log.Info("目标库: %s:%d", cfg.Target.Host, cfg.Target.PortOrDefault())
	log.Info("============================================================")

	// 信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Info("收到中断信号，程序退出")
		os.Exit(0)
	}()

	// 执行同步
	var fullErr error

	if *mode == "full" || *mode == "all" {
		if cfg.Sync.FullSync.Enabled {
			log.Info("开始全量同步...")
			mgr := fullsync.New(cfg, log)
			fullErr = mgr.Run()
			if fullErr != nil {
				log.Error("全量同步失败: %v", fullErr)
			} else {
				log.Info("全量同步完成")
			}
		} else {
			log.Info("全量同步已禁用，跳过")
		}
	}

	if (*mode == "incremental" || *mode == "all") && fullErr == nil {
		if cfg.Sync.Incremental.Enabled {
			log.Info("开始增量同步（监听 Binlog）...")
			mgr, err := incremental.NewIncrementalSyncManager(cfg, log)
			if err != nil {
				log.Error("创建增量同步管理器失败: %v", err)
				os.Exit(1)
			}
			defer mgr.Close()

			if *resetPosition {
				log.Warn("--reset-position 已指定，将重置 Binlog 位点")
				mgr.ResetPosition()
			}

			incErr := mgr.Run()
			if incErr != nil {
				log.Error("增量同步失败: %v", incErr)
				os.Exit(1)
			}
		} else {
			log.Info("增量同步已禁用，跳过")
		}
	}

	if fullErr != nil {
		os.Exit(1)
	}
}

// setupLogging 初始化日志（同时输出到控制台和文件）
func setupLogging(cfg *config.Config) *logger.Logger {
	level := logger.ParseLevel(cfg.Logging.Level)

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

	// 使用标准库 log 作为底层
	baseLogger := log.New(multiWriter, "", log.Ldate|log.Ltime)
	_ = baseLogger // logger 包自己处理格式

	return logger.New(multiWriter, level)
}
