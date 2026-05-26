package checkenv

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"go_mysql_sync/config"
)

// Run 执行环境检查
func Run(configPath string) int {
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("加载配置文件失败: %v", err)
	}

	fmt.Println("MySQL 同步程序 - 环境检查")

	var hasError bool

	// 检查源库
	if err := checkSource(cfg); err != nil {
		log.Printf("源库检查异常: %v", err)
		hasError = true
	}

	// 检查目标库
	if err := checkTarget(cfg); err != nil {
		log.Printf("目标库检查异常: %v", err)
		hasError = true
	}

	fmt.Println(strings.Repeat("=", 50))
	if hasError {
		fmt.Println("检查结果: 存在问题，请修复后再运行同步程序")
		return 1
	}
	fmt.Println("检查结果: 一切正常，可以运行同步程序 ✓")
	return 0
}

func checkSource(cfg *config.Config) error {
	src := cfg.Source
	fmt.Printf("\n%s\n", strings.Repeat("=", 50))
	fmt.Printf("检查源库: %s:%d\n", src.Host, src.PortOrDefault())
	fmt.Println(strings.Repeat("=", 50))

	conn, err := sql.Open("mysql", src.DSN())
	if err != nil {
		fmt.Printf("✗ 连接失败: %v\n", err)
		return err
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		fmt.Printf("✗ 连接失败: %v\n", err)
		return err
	}
	fmt.Println("✓ 连接成功")

	// 检查 log_bin
	var logBin string
	if err := conn.QueryRow("SHOW VARIABLES LIKE 'log_bin'").Scan(&logBin, &logBin); err != nil {
		fmt.Println("✗ 查询 log_bin 失败")
	} else if strings.ToUpper(logBin) == "ON" {
		fmt.Println("✓ log_bin = ON")
	} else {
		fmt.Println("✗ log_bin 未开启！请在源库 my.cnf 中设置 log_bin=ON")
	}

	// 检查 binlog_format
	var binlogFormat string
	if err := conn.QueryRow("SHOW VARIABLES LIKE 'binlog_format'").Scan(&binlogFormat, &binlogFormat); err != nil {
		fmt.Println("✗ 查询 binlog_format 失败")
	} else if strings.ToUpper(binlogFormat) == "ROW" {
		fmt.Printf("✓ binlog_format = ROW\n")
	} else {
		fmt.Printf("✗ binlog_format = %s（需要 ROW 格式）\n", binlogFormat)
		fmt.Println("  请在源库 my.cnf 中设置 binlog_format=ROW")
	}

	// 检查 binlog_row_image
	var rowImage string
	if err := conn.QueryRow("SHOW VARIABLES LIKE 'binlog_row_image'").Scan(&rowImage, &rowImage); err != nil {
		fmt.Println("✗ 查询 binlog_row_image 失败")
	} else {
		fmt.Printf("✓ binlog_row_image = %s\n", rowImage)
	}

	// 获取 Binlog 位点
	var logFile string
	var logPos uint32
	if err := conn.QueryRow("SHOW MASTER STATUS").Scan(&logFile, &logPos); err != nil {
		fmt.Println("✗ 无法获取 Binlog 状态")
	} else {
		fmt.Printf("✓ 当前 Binlog: %s:%d\n", logFile, logPos)
	}

	// 检查权限
	var grants string
	if err := conn.QueryRow("SHOW GRANTS FOR CURRENT_USER()").Scan(&grants); err != nil {
		// 可能有多个 grant 行
		fmt.Println("✗ 查询权限失败")
	} else {
		grants = strings.ToUpper(grants)
		if strings.Contains(grants, "REPLICATION SLAVE") || strings.Contains(grants, "ALL PRIVILEGES") {
			fmt.Println("✓ REPLICATION SLAVE 权限")
		} else {
			fmt.Println("✗ 缺少 REPLICATION SLAVE 权限")
		}
		if strings.Contains(grants, "REPLICATION CLIENT") || strings.Contains(grants, "ALL PRIVILEGES") {
			fmt.Println("✓ REPLICATION CLIENT 权限")
		} else {
			fmt.Println("✗ 缺少 REPLICATION CLIENT 权限")
		}
	}

	// 检查 server_id 冲突
	var serverID int
	if err := conn.QueryRow("SHOW VARIABLES LIKE 'server_id'").Scan(&serverID, &serverID); err == nil {
		syncServerID := cfg.Sync.ServerIDOrDefault()
		if serverID != int(syncServerID) {
			fmt.Printf("✓ server_id 不冲突 (源库=%d, 伪从库=%d)\n", serverID, syncServerID)
		} else {
			fmt.Printf("✗ server_id 冲突！源库 server_id=%d，请修改配置中 sync.incremental.server_id\n", serverID)
		}
	}

	return nil
}

func checkTarget(cfg *config.Config) error {
	tgt := cfg.Target
	fmt.Printf("\n%s\n", strings.Repeat("=", 50))
	fmt.Printf("检查目标库: %s:%d\n", tgt.Host, tgt.PortOrDefault())
	fmt.Println(strings.Repeat("=", 50))

	conn, err := sql.Open("mysql", tgt.DSN())
	if err != nil {
		fmt.Printf("✗ 连接失败: %v\n", err)
		return err
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		fmt.Printf("✗ 连接失败: %v\n", err)
		return err
	}
	fmt.Println("✓ 连接成功")

	// 检查权限
	rows, err := conn.Query("SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		fmt.Printf("✗ 查询权限失败: %v\n", err)
		return err
	}
	defer rows.Close()

	var allGrants strings.Builder
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			continue
		}
		allGrants.WriteString(grant)
		allGrants.WriteString(" ")
	}
	grants := strings.ToUpper(allGrants.String())

	for _, priv := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "INDEX", "ALTER"} {
		if strings.Contains(grants, priv) || strings.Contains(grants, "ALL PRIVILEGES") {
			fmt.Printf("✓ %s 权限\n", priv)
		} else {
			fmt.Printf("✗ %s 权限\n", priv)
		}
	}

	return nil
}
