package incremental

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"go_mysql_sync/config"
)

// IncrementalSyncManager 增量同步管理器
type IncrementalSyncManager struct {
	cfg       *config.Config
	position  *BinlogPosition
	writer    *TargetWriter
	eventChan chan DMLEvent
	running   bool

	// 过滤配置
	onlySchemas    map[string]bool
	excludeTables  map[string]bool
}

// NewIncrementalSyncManager 创建增量同步管理器
func NewIncrementalSyncManager(cfg *config.Config) (*IncrementalSyncManager, error) {
	writer, err := NewTargetWriter(cfg)
	if err != nil {
		return nil, fmt.Errorf("创建目标库写入器失败: %w", err)
	}

	mgr := &IncrementalSyncManager{
		cfg:       cfg,
		position:  NewBinlogPosition(cfg.Sync.PositionFileOrDefault()),
		writer:    writer,
		eventChan: make(chan DMLEvent, 10000),
		running:   true,
	}

	// 初始化过滤配置
	mgr.onlySchemas = make(map[string]bool)
	for _, db := range cfg.Sync.Databases {
		mgr.onlySchemas[db] = true
	}

	mgr.excludeTables = make(map[string]bool)
	for _, t := range cfg.Sync.ExcludeTables {
		mgr.excludeTables[t] = true
	}

	return mgr, nil
}

// ResetPosition 重置位点
func (m *IncrementalSyncManager) ResetPosition() {
	m.position.Reset()
}

// shouldSync 判断该表是否需要同步
func (m *IncrementalSyncManager) shouldSync(schema, table string) bool {
	if len(m.onlySchemas) > 0 && !m.onlySchemas[schema] {
		return false
	}
	fqn := fmt.Sprintf("%s.%s", schema, table)
	if m.excludeTables[fqn] {
		return false
	}
	return true
}

// myEventHandler 自定义 Binlog 事件处理器
type myEventHandler struct {
	mgr *IncrementalSyncManager
}

func (h *myEventHandler) OnRotate(e *replication.RotateEvent) {
	pos := e.Position
	h.mgr.position.Update(string(e.NextLogName), uint32(pos))
	slog.Info("Binlog 文件切换", "file", string(e.NextLogName), "pos", pos)
}

func (h *myEventHandler) OnDDL(nextPos mysql.Position, e *replication.QueryEvent) error {
	slog.Debug("DDL 事件", "query", string(e.Query))
	return nil
}

func (h *myEventHandler) OnXID(nextPos mysql.Position) error {
	return nil
}

func (h *myEventHandler) OnRow(e *canal.RowsEvent) error {
	schema := e.Table.Schema
	table := e.Table.Name

	if !h.mgr.shouldSync(schema, table) {
		return nil
	}

	// 将 []interface{} 转为 []string (列名) 和 map[string]interface{} (行数据)
	makeRowMap := func(values []interface{}) map[string]interface{} {
		m := make(map[string]interface{}, len(e.Table.Columns))
		for i, col := range e.Table.Columns {
			if i < len(values) {
				m[col.Name] = values[i]
			}
		}
		return m
	}

	switch e.Action {
	case canal.InsertAction:
		h.mgr.eventChan <- DMLEvent{
			Type:   "INSERT",
			Schema: schema,
			Table:  table,
			After:  makeRowMap(e.Rows[0]),
		}

	case canal.UpdateAction:
		if len(e.Rows) >= 2 {
			h.mgr.eventChan <- DMLEvent{
				Type:   "UPDATE",
				Schema: schema,
				Table:  table,
				Before: makeRowMap(e.Rows[0]),
				After:  makeRowMap(e.Rows[1]),
			}
		}

	case canal.DeleteAction:
		h.mgr.eventChan <- DMLEvent{
			Type:   "DELETE",
			Schema: schema,
			Table:  table,
			Before: makeRowMap(e.Rows[0]),
		}
	}

	return nil
}

func (h *myEventHandler) OnGTID(gtid mysql.GTID) error {
	return nil
}

func (h *myEventHandler) OnPosSynced(pos mysql.Position, gs mysql.GTIDSet, force bool) error {
	return nil
}

func (h *myEventHandler) OnTableChanged(schema, table string) error {
	return nil
}

func (h *myEventHandler) String() string {
	return "mysql-sync-handler"
}

// Run 启动增量同步
func (m *IncrementalSyncManager) Run() error {
	// 启动消费者 goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go m.consumer(ctx)

	// 配置 canal
	cfg := canal.NewDefaultConfig()
	cfg.Addr = m.cfg.Source.Addr()
	cfg.User = m.cfg.Source.User
	cfg.Password = m.cfg.Source.Password
	cfg.Flavor = "mysql"
	cfg.ServerID = m.cfg.Sync.ServerIDOrDefault()
	// 不使用 canal 自带的 dump 功能
	cfg.Dump.ExecutionPath = ""

	// 过滤数据库
	if len(m.cfg.Sync.Databases) > 0 {
		cfg.IncludeTableRegex = make([]string, 0)
		for _, db := range m.cfg.Sync.Databases {
			cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, fmt.Sprintf("^%s\\..*", db))
		}
	}

	// 排除表
	if len(m.cfg.Sync.ExcludeTables) > 0 {
		cfg.ExcludeTableRegex = make([]string, 0)
		for _, t := range m.cfg.Sync.ExcludeTables {
			cfg.ExcludeTableRegex = append(cfg.ExcludeTableRegex, fmt.Sprintf("^%s$", t))
		}
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("创建 canal 失败: %w", err)
	}

	// 注册事件处理器
	handler := &myEventHandler{mgr: m}
	c.SetEventHandler(handler)

	// 获取起始位点
	logFile, logPos := m.position.Get()

	// 监听位点更新
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if !m.running {
				return
			}
			// 从 canal 获取当前同步位点并保存
			pos := c.SyncedPosition()
			m.position.Update(pos.Name, uint32(pos.Pos))
			if err := m.position.Save(); err != nil {
				slog.Error("保存位点失败", "error", err)
			}
		}
	}()

	slog.Info("开始监听 Binlog...",
		"host", m.cfg.Source.Host,
		"port", m.cfg.Source.PortOrDefault(),
		"server_id", m.cfg.Sync.ServerIDOrDefault(),
		"log_file", logFile,
		"log_pos", logPos,
	)

	// 从指定位点开始同步
	var startErr error
	if logFile != "" && logPos > 0 {
		pos := mysql.Position{Name: logFile, Pos: uint32(logPos)}
		startErr = c.RunFrom(pos)
	} else {
		startErr = c.Run()
	}

	if startErr != nil {
		if !strings.Contains(startErr.Error(), "context canceled") {
			return fmt.Errorf("Binlog 同步异常: %w", startErr)
		}
	}

	// 停止 canal
	c.Close()
	return nil
}

// consumer 批量消费事件并写入目标库
func (m *IncrementalSyncManager) consumer(ctx context.Context) {
	batchSize := m.cfg.Sync.BatchSizeOrDefault()
	batchTimeout := time.Duration(m.cfg.Sync.BatchTimeoutOrDefault()) * time.Second

	var batch []DMLEvent
	timer := time.NewTimer(batchTimeout)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := m.writer.ApplyBatch(batch); err != nil {
			slog.Error("批量写入失败", "count", len(batch), "error", err)
		} else {
			slog.Debug("已提交事件", "count", len(batch))
		}
		batch = batch[:0]
		timer.Reset(batchTimeout)
	}

	for {
		select {
		case <-ctx.Done():
			// 刷新剩余数据
			flush()
			slog.Info("消费者退出")
			return

		case evt := <-m.eventChan:
			batch = append(batch, evt)
			if len(batch) >= batchSize {
				flush()
			}

		case <-timer.C:
			if len(batch) > 0 {
				flush()
			}
			timer.Reset(batchTimeout)
		}
	}
}

// Close 关闭增量同步
func (m *IncrementalSyncManager) Close() {
	m.running = false
	close(m.eventChan)
	m.writer.Close()
}
