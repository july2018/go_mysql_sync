package fullsync

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"go_mysql_sync/config"
	"go_mysql_sync/logger"
)

// FullSyncManager 全量同步管理器
type FullSyncManager struct {
	cfg           *config.Config
	log           *logger.Logger
	mysqldumpBin  string
	mysqlBin      string
	positionFile  string
}

// New 创建全量同步管理器
func New(cfg *config.Config, log *logger.Logger) *FullSyncManager {
	dumpBin := findBin(cfg.Sync.FullSync.MysqldumpBin, "mysqldump", log)
	mysqlBin := findBin(cfg.Sync.FullSync.MysqlBin, "mysql", log)
	return &FullSyncManager{
		cfg:          cfg,
		log:          log,
		mysqldumpBin: dumpBin,
		mysqlBin:     mysqlBin,
		positionFile: cfg.Sync.PositionFileOrDefault(),
	}
}

// findBin 查找可执行文件路径
func findBin(configuredPath, name string, log *logger.Logger) string {
	if configuredPath != "" {
		if _, err := os.Stat(configuredPath); err == nil {
			return configuredPath
		}
	}

	// 在 PATH 中查找
	if path, err := exec.LookPath(name); err == nil {
		return path
	}

	// Windows 常见路径
	if runtime.GOOS == "windows" {
		candidates := []string{
			fmt.Sprintf(`C:\Program Files\MySQL\MySQL Server 8.0\bin\%s.exe`, name),
			fmt.Sprintf(`C:\Program Files\MySQL\MySQL Server 5.7\bin\%s.exe`, name),
			fmt.Sprintf(`C:\xampp\mysql\bin\%s.exe`, name),
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				return c
			}
		}
	}

	// Linux 常见路径
	if runtime.GOOS == "linux" {
		candidates := []string{
			fmt.Sprintf("/usr/bin/%s", name),
			fmt.Sprintf("/usr/local/mysql/bin/%s", name),
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				return c
			}
		}
	}

	log.Fatal("找不到 %s，请在配置文件 full_sync.%s_bin 中指定路径", name, name)
	return ""
}

// sourceConn 获取源库连接
func (m *FullSyncManager) sourceConn() (*sql.DB, error) {
	return sql.Open("mysql", m.cfg.Source.DSN())
}

// targetConn 获取目标库连接
func (m *FullSyncManager) targetConn() (*sql.DB, error) {
	return sql.Open("mysql", m.cfg.Target.DSN())
}

// getDatabases 获取需要同步的数据库列表
func (m *FullSyncManager) getDatabases() ([]string, error) {
	if len(m.cfg.Sync.Databases) > 0 {
		return m.cfg.Sync.Databases, nil
	}

	conn, err := m.sourceConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	systemDBs := map[string]bool{
		"information_schema": true,
		"performance_schema": true,
		"mysql":              true,
		"sys":                true,
	}

	var dbs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if !systemDBs[name] {
			dbs = append(dbs, name)
		}
	}
	return dbs, rows.Err()
}

// getBinlogPosition 获取源库当前 Binlog 位点
func (m *FullSyncManager) getBinlogPosition() (string, uint32, error) {
	conn, err := m.sourceConn()
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()

	var logFile string
	var logPos uint32
	err = conn.QueryRow("SHOW MASTER STATUS").Scan(&logFile, &logPos)
	if err != nil {
		return "", 0, fmt.Errorf("无法获取 Binlog 位点，请确认源库已开启 Binlog（log_bin=ON）: %w", err)
	}
	return logFile, logPos, nil
}

// binlogPosition 位点数据
type binlogPosition struct {
	LogFile   string `json:"log_file"`
	LogPos    uint32 `json:"log_pos"`
	Timestamp string `json:"timestamp"`
	Note      string `json:"note,omitempty"`
}

// savePosition 保存 Binlog 位点到文件
func (m *FullSyncManager) savePosition(logFile string, logPos uint32) error {
	dir := filepath.Dir(m.positionFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data := binlogPosition{
		LogFile:   logFile,
		LogPos:    logPos,
		Timestamp: time.Now().Format("2006-01-02 15:04:05"),
		Note:      "由全量同步完成后自动记录",
	}

	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// 原子写入
	tmpPath := m.positionFile + ".tmp"
	if err := os.WriteFile(tmpPath, jsonBytes, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, m.positionFile)
}

// ensureDatabaseExists 在目标库创建数据库（若不存在）
func (m *FullSyncManager) ensureDatabaseExists(dbName string) error {
	conn, err := m.targetConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Exec(fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", dbName,
	))
	return err
}

// dumpAndRestore 对单个数据库执行 mysqldump + mysql 导入
func (m *FullSyncManager) dumpAndRestore(dbName string) error {
	logger := m.log.With("database", dbName)
	logger.Info("开始全量导出...")

	// 构建 mysqldump 命令
	dumpArgs := []string{
		fmt.Sprintf("-h%s", m.cfg.Source.Host),
		fmt.Sprintf("-P%d", m.cfg.Source.PortOrDefault()),
		fmt.Sprintf("-u%s", m.cfg.Source.User),
		fmt.Sprintf("-p%s", m.cfg.Source.Password),
		"--single-transaction",
		"--master-data=2",
		"--set-gtid-purged=OFF",
		"--skip-lock-tables",
		"--default-character-set=utf8mb4",
		"--hex-blob",
		"--routines",
		"--triggers",
		"--add-drop-table",
	}

	// 排除特定表
	for _, item := range m.cfg.Sync.ExcludeTables {
		parts := splitTable(item)
		if len(parts) == 2 && parts[0] == dbName {
			dumpArgs = append(dumpArgs, fmt.Sprintf("--ignore-table=%s", item))
		}
	}
	dumpArgs = append(dumpArgs, dbName)

	// 构建 mysql 导入命令
	restoreArgs := []string{
		fmt.Sprintf("-h%s", m.cfg.Target.Host),
		fmt.Sprintf("-P%d", m.cfg.Target.PortOrDefault()),
		fmt.Sprintf("-u%s", m.cfg.Target.User),
		fmt.Sprintf("-p%s", m.cfg.Target.Password),
		"--default-character-set=utf8mb4",
		dbName,
	}

	// 确保目标库存在
	if err := m.ensureDatabaseExists(dbName); err != nil {
		return fmt.Errorf("创建目标数据库失败: %w", err)
	}

	// 执行 mysqldump | mysql 管道
	dumpCmd := exec.Command(m.mysqldumpBin, dumpArgs...)
	restoreCmd := exec.Command(m.mysqlBin, restoreArgs...)

	// 连接管道
	pipe, err := dumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("创建管道失败: %w", err)
	}
	restoreCmd.Stdin = pipe

	// 收集错误输出
	var dumpErr, restoreErr []byte
	dumpCmd.Stderr = &bytesWriter{data: &dumpErr}
	restoreCmd.Stderr = &bytesWriter{data: &restoreErr}

	if err := dumpCmd.Start(); err != nil {
		return fmt.Errorf("启动 mysqldump 失败: %w", err)
	}
	if err := restoreCmd.Start(); err != nil {
		dumpCmd.Process.Kill()
		return fmt.Errorf("启动 mysql 失败: %w", err)
	}

	// 等待 dump 完成
	if err := dumpCmd.Wait(); err != nil {
		restoreCmd.Process.Kill()
		return fmt.Errorf("mysqldump 失败: %s", string(dumpErr))
	}

	// 等待 restore 完成
	if err := restoreCmd.Wait(); err != nil {
		return fmt.Errorf("mysql 导入失败: %s", string(restoreErr))
	}

	logger.Info("全量同步完成")
	return nil
}

// Run 执行全量同步
func (m *FullSyncManager) Run() error {
	databases, err := m.getDatabases()
	if err != nil {
		return err
	}
	if len(databases) == 0 {
		m.log.Warn("没有找到需要同步的数据库，全量同步跳过")
		return nil
	}

	m.log.Info("需要同步的数据库: %v", databases)

	// 记录 Binlog 位点
	logFile, logPos, err := m.getBinlogPosition()
	if err != nil {
		return err
	}
	m.log.Info("当前 Binlog 位点: file=%s pos=%d", logFile, logPos)

	startTime := time.Now()
	parallel := m.cfg.Sync.ParallelOrDefault()

	var mu sync.Mutex
	var successCount, failCount int
	var wg sync.WaitGroup

	// 使用带缓冲的 channel 控制并发
	sem := make(chan struct{}, parallel)

	for _, db := range databases {
		wg.Add(1)
		sem <- struct{}{} // 获取信号量

		go func(dbName string) {
			defer wg.Done()
			defer func() { <-sem }() // 释放信号量

			if err := m.dumpAndRestore(dbName); err != nil {
				mu.Lock()
				failCount++
				mu.Unlock()
				m.log.Error("全量同步失败 [%s]: %v", dbName, err)
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(db)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	m.log.Info("全量同步完成: success=%d failed=%d elapsed=%s",
		successCount, failCount, elapsed.Round(time.Second).String())

	if failCount > 0 {
		return fmt.Errorf("有 %d 个数据库全量同步失败，请检查日志", failCount)
	}

	// 保存 Binlog 位点
	if err := m.savePosition(logFile, logPos); err != nil {
		return fmt.Errorf("保存位点文件失败: %w", err)
	}
	m.log.Info("Binlog 位点已保存: file=%s pos=%d path=%s", logFile, logPos, m.positionFile)

	return nil
}

// bytesWriter 用于收集命令的错误输出
type bytesWriter struct {
	data *[]byte
}

func (w *bytesWriter) Write(p []byte) (int, error) {
	*w.data = append(*w.data, p...)
	return len(p), nil
}

// splitTable 按 "." 分割表名
func splitTable(fqn string) []string {
	for i, c := range fqn {
		if c == '.' {
			return []string{fqn[:i], fqn[i+1:]}
		}
	}
	return []string{fqn}
}
