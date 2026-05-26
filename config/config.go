package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config 顶层配置
type Config struct {
	Source  SourceConfig  `yaml:"source"`
	Target  TargetConfig  `yaml:"target"`
	Sync    SyncConfig    `yaml:"sync"`
	Logging LoggingConfig `yaml:"logging"`
}

// SourceConfig 源数据库（主库）配置
type SourceConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Charset  string `yaml:"charset"`
}

// TargetConfig 目标数据库配置
type TargetConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Charset  string `yaml:"charset"`
}

// SyncConfig 同步配置
type SyncConfig struct {
	Databases     []string      `yaml:"databases"`
	ExcludeTables []string      `yaml:"exclude_tables"`
	FullSync      FullSyncCfg   `yaml:"full_sync"`
	Incremental   IncrSyncCfg   `yaml:"incremental"`
}

// FullSyncCfg 全量同步配置
type FullSyncCfg struct {
	Enabled      bool   `yaml:"enabled"`
	MysqldumpBin string `yaml:"mysqldump_bin"`
	MysqlBin     string `yaml:"mysql_bin"`
	ChunkSize    int    `yaml:"chunk_size"`
	Parallel     int    `yaml:"parallel"`
}

// IncrSyncCfg 增量同步配置
type IncrSyncCfg struct {
	Enabled            bool   `yaml:"enabled"`
	ServerID           uint32 `yaml:"server_id"`
	PositionFile       string `yaml:"position_file"`
	ReconnectInterval  int    `yaml:"reconnect_interval"`
	BatchSize          int    `yaml:"batch_size"`
	BatchTimeout       int    `yaml:"batch_timeout"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level       string `yaml:"level"`
	File        string `yaml:"file"`
	MaxBytes    int    `yaml:"max_bytes"`
	BackupCount int    `yaml:"backup_count"`
}

// DSN 返回 MySQL 连接字符串
func (s *SourceConfig) DSN() string {
	if s.Port == 0 {
		s.Port = 3306
	}
	charset := s.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s&timeout=10s",
		s.User, s.Password, s.Host, s.Port, charset)
}

// DSN 返回 MySQL 连接字符串
func (t *TargetConfig) DSN() string {
	if t.Port == 0 {
		t.Port = 3306
	}
	charset := t.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s&timeout=10s&multiStatements=true",
		t.User, t.Password, t.Host, t.Port, charset)
}

// DSNWithDB 返回带数据库名的连接字符串
func (t *TargetConfig) DSNWithDB(db string) string {
	if t.Port == 0 {
		t.Port = 3306
	}
	charset := t.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&timeout=10s&multiStatements=true",
		t.User, t.Password, t.Host, t.Port, db, charset)
}

// Addr 返回 host:port 格式
func (s *SourceConfig) Addr() string {
	if s.Port == 0 {
		s.Port = 3306
	}
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

// PortOrDefault 返回端口，默认 3306
func (s *SourceConfig) PortOrDefault() int {
	if s.Port == 0 {
		return 3306
	}
	return s.Port
}

// PortOrDefault 返回端口，默认 3306
func (t *TargetConfig) PortOrDefault() int {
	if t.Port == 0 {
		return 3306
	}
	return t.Port
}

// PositionFileOrDefault 返回位点文件路径
func (s *SyncConfig) PositionFileOrDefault() string {
	if s.Incremental.PositionFile == "" {
		return "logs/binlog_position.json"
	}
	return s.Incremental.PositionFile
}

// ParallelOrDefault 返回并行数，默认 2
func (s *SyncConfig) ParallelOrDefault() int {
	if s.FullSync.Parallel <= 0 {
		return 2
	}
	return s.FullSync.Parallel
}

// BatchSizeOrDefault 返回批量大小，默认 100
func (s *SyncConfig) BatchSizeOrDefault() int {
	if s.Incremental.BatchSize <= 0 {
		return 100
	}
	return s.Incremental.BatchSize
}

// BatchTimeoutOrDefault 返回批量超时秒数，默认 2
func (s *SyncConfig) BatchTimeoutOrDefault() int {
	if s.Incremental.BatchTimeout <= 0 {
		return 2
	}
	return s.Incremental.BatchTimeout
}

// ReconnectIntervalOrDefault 返回重连间隔秒数，默认 5
func (s *SyncConfig) ReconnectIntervalOrDefault() int {
	if s.Incremental.ReconnectInterval <= 0 {
		return 5
	}
	return s.Incremental.ReconnectInterval
}

// ServerIDOrDefault 返回 server_id，默认 9999
func (s *SyncConfig) ServerIDOrDefault() uint32 {
	if s.Incremental.ServerID == 0 {
		return 9999
	}
	return s.Incremental.ServerID
}

// Load 从 YAML 文件加载配置
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 设置默认值
	if cfg.Source.Charset == "" {
		cfg.Source.Charset = "utf8mb4"
	}
	if cfg.Target.Charset == "" {
		cfg.Target.Charset = "utf8mb4"
	}
	if cfg.Logging.File == "" {
		cfg.Logging.File = "logs/sync.log"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "INFO"
	}
	if cfg.Logging.MaxBytes == 0 {
		cfg.Logging.MaxBytes = 10 * 1024 * 1024
	}
	if cfg.Logging.BackupCount == 0 {
		cfg.Logging.BackupCount = 5
	}
	if cfg.Incremental.PositionFile == "" {
		cfg.Incremental.PositionFile = "logs/binlog_position.json"
	}

	return cfg, nil
}
