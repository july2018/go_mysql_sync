package incremental

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"

	"go_mysql_sync/config"
	"go_mysql_sync/logger"
)

// DMLEvent 表示一条 DML 事件
type DMLEvent struct {
	Type   string // "INSERT" / "UPDATE" / "DELETE"
	Schema string
	Table  string
	Before map[string]interface{} // UPDATE/DELETE 前的值
	After  map[string]interface{} // INSERT/UPDATE 后的值
}

// TargetWriter 目标库批量写入器
type TargetWriter struct {
	cfg     *config.Config
	log     *logger.Logger
	db      *sql.DB
	pkCache map[string][]string // "schema.table" -> [pk_col1, pk_col2]
	mu      sync.RWMutex
}

// NewTargetWriter 创建目标库写入器
func NewTargetWriter(cfg *config.Config, log *logger.Logger) (*TargetWriter, error) {
	db, err := sql.Open("mysql", cfg.Target.DSN())
	if err != nil {
		return nil, err
	}
	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	log.Info("目标库连接建立: %s:%d", cfg.Target.Host, cfg.Target.PortOrDefault())
	return &TargetWriter{cfg: cfg, log: log, db: db, pkCache: make(map[string][]string)}, nil
}

// ensureConnected 确保数据库连接可用
func (w *TargetWriter) ensureConnected() error {
	if err := w.db.Ping(); err != nil {
		w.log.Warn("目标库连接断开，尝试重连...")
		if err := w.db.Close(); err != nil {
			// ignore close error
		}
		db, err := sql.Open("mysql", w.cfg.Target.DSN())
		if err != nil {
			return fmt.Errorf("重连失败: %w", err)
		}
		if err := db.Ping(); err != nil {
			return fmt.Errorf("重连后 ping 失败: %w", err)
		}
		w.db = db
		w.log.Info("目标库重连成功")
	}
	return nil
}

// getPrimaryKeys 获取表的主键列（带缓存）
func (w *TargetWriter) getPrimaryKeys(schema, table string) ([]string, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	// 读缓存
	w.mu.RLock()
	if pk, ok := w.pkCache[key]; ok {
		w.mu.RUnlock()
		return pk, nil
	}
	w.mu.RUnlock()

	if err := w.ensureConnected(); err != nil {
		return nil, err
	}

	query := `SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`

	rows, err := w.db.Query(query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("查询主键失败: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pks = append(pks, col)
	}

	// 写缓存
	w.mu.Lock()
	w.pkCache[key] = pks
	w.mu.Unlock()

	return pks, nil
}

// buildInsertSQL 构建 INSERT ... ON DUPLICATE KEY UPDATE SQL
func buildInsertSQL(schema, table string, row map[string]interface{}) (string, []interface{}) {
	cols := make([]string, 0, len(row))
	placeholders := make([]string, 0, len(row))
	values := make([]interface{}, 0, len(row))

	for col, val := range row {
		cols = append(cols, fmt.Sprintf("`%s`", col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	colsStr := strings.Join(cols, ", ")
	placeholdersStr := strings.Join(placeholders, ", ")

	// ON DUPLICATE KEY UPDATE `col`=VALUES(`col`)
	updateParts := make([]string, 0, len(cols))
	for _, col := range cols {
		updateParts = append(updateParts, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}
	updateStr := strings.Join(updateParts, ", ")

	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		schema, table, colsStr, placeholdersStr, updateStr)

	return sql, values
}

// buildUpdateSQL 构建 UPDATE SQL
func buildUpdateSQL(schema, table string, before, after map[string]interface{}, pkCols []string) (string, []interface{}) {
	setParts := make([]string, 0, len(after))
	values := make([]interface{}, 0)

	for col, val := range after {
		setParts = append(setParts, fmt.Sprintf("`%s`=?", col))
		values = append(values, val)
	}

	whereParts := make([]string, 0, len(pkCols))
	for _, pk := range pkCols {
		whereParts = append(whereParts, fmt.Sprintf("`%s`=?", pk))
		values = append(values, before[pk])
	}

	sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s",
		schema, table, strings.Join(setParts, ", "), strings.Join(whereParts, " AND "))

	return sql, values
}

// buildDeleteSQL 构建 DELETE SQL
func buildDeleteSQL(schema, table string, row map[string]interface{}, pkCols []string) (string, []interface{}) {
	whereParts := make([]string, 0, len(pkCols))
	values := make([]interface{}, 0)

	for _, pk := range pkCols {
		whereParts = append(whereParts, fmt.Sprintf("`%s`=?", pk))
		values = append(values, row[pk])
	}

	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s",
		schema, table, strings.Join(whereParts, " AND "))

	return sql, values
}

// ApplyBatch 批量执行 DML 事件
func (w *TargetWriter) ApplyBatch(events []DMLEvent) error {
	if len(events) == 0 {
		return nil
	}

	if err := w.ensureConnected(); err != nil {
		return err
	}

	tx, err := w.db.Begin()
	if err != nil {
		return fmt.Errorf("开始事务失败: %w", err)
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	for _, evt := range events {
		var sqlStr string
		var args []interface{}

		switch evt.Type {
		case "INSERT":
			sqlStr, args = buildInsertSQL(evt.Schema, evt.Table, evt.After)

		case "UPDATE":
			pkCols, err := w.getPrimaryKeys(evt.Schema, evt.Table)
			if err != nil {
				return fmt.Errorf("获取主键失败 [%s.%s]: %w", evt.Schema, evt.Table, err)
			}
			if len(pkCols) == 0 {
				// 无主键时退化为全列匹配
				for k := range evt.Before {
					pkCols = append(pkCols, k)
				}
			}
			sqlStr, args = buildUpdateSQL(evt.Schema, evt.Table, evt.Before, evt.After, pkCols)

		case "DELETE":
			pkCols, err := w.getPrimaryKeys(evt.Schema, evt.Table)
			if err != nil {
				return fmt.Errorf("获取主键失败 [%s.%s]: %w", evt.Schema, evt.Table, err)
			}
			if len(pkCols) == 0 {
				for k := range evt.After {
					pkCols = append(pkCols, k)
				}
			}
			sqlStr, args = buildDeleteSQL(evt.Schema, evt.Table, evt.After, pkCols)
		}

		if _, err := tx.Exec(sqlStr, args...); err != nil {
			return fmt.Errorf("执行 %s 失败 [%s.%s]: %w", evt.Type, evt.Schema, evt.Table, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

// Close 关闭连接
func (w *TargetWriter) Close() {
	if w.db != nil {
		w.db.Close()
	}
}
