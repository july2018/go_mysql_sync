package incremental

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// BinlogPosition Binlog 位点管理器（持久化到文件）
type BinlogPosition struct {
	mu           sync.Mutex
	positionFile string
	LogFile      string
	LogPos       uint32
}

// NewBinlogPosition 创建位点管理器
func NewBinlogPosition(positionFile string) *BinlogPosition {
	bp := &BinlogPosition{
		positionFile: positionFile,
	}
	bp.load()
	return bp
}

// PositionData 位点持久化数据
type PositionData struct {
	LogFile   string `json:"log_file"`
	LogPos    uint32 `json:"log_pos"`
	Timestamp string `json:"timestamp"`
}

// load 从文件加载位点
func (bp *BinlogPosition) load() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	data, err := os.ReadFile(bp.positionFile)
	if err != nil {
		return // 文件不存在，从当前位点开始
	}

	var pos PositionData
	if err := json.Unmarshal(data, &pos); err != nil {
		slog.Warn("加载位点文件失败，将从头读取", "error", err)
		return
	}

	bp.LogFile = pos.LogFile
	bp.LogPos = pos.LogPos
	slog.Info("已加载 Binlog 位点",
		"file", bp.LogFile,
		"pos", bp.LogPos,
		"recorded_at", pos.Timestamp,
	)
}

// Get 获取当前位点
func (bp *BinlogPosition) Get() (string, uint32) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.LogFile, bp.LogPos
}

// Update 更新内存中的位点（不持久化）
func (bp *BinlogPosition) Update(logFile string, logPos uint32) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.LogFile = logFile
	bp.LogPos = logPos
}

// Save 持久化位点到文件（原子写入）
func (bp *BinlogPosition) Save() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.LogFile == "" {
		return nil
	}

	dir := filepath.Dir(bp.positionFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data := PositionData{
		LogFile:   bp.LogFile,
		LogPos:    bp.LogPos,
		Timestamp: time.Now().Format("2006-01-02 15:04:05"),
	}

	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化位点数据失败: %w", err)
	}

	// 原子写入：先写临时文件，再 rename
	tmpPath := bp.positionFile + ".tmp"
	if err := os.WriteFile(tmpPath, jsonBytes, 0644); err != nil {
		return fmt.Errorf("写入临时位点文件失败: %w", err)
	}

	if err := os.Rename(tmpPath, bp.positionFile); err != nil {
		return fmt.Errorf("重命名位点文件失败: %w", err)
	}

	return nil
}

// Reset 删除位点文件，下次从当前位置开始
func (bp *BinlogPosition) Reset() {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	bp.LogFile = ""
	bp.LogPos = 0

	if _, err := os.Stat(bp.positionFile); err == nil {
		if err := os.Remove(bp.positionFile); err != nil {
			slog.Warn("删除位点文件失败", "error", err)
		} else {
			slog.Warn("已删除 Binlog 位点文件", "path", bp.positionFile)
		}
	}
}
