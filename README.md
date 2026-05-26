# go_mysql_sync

Go 版 MySQL 数据同步工具，通过**伪装成从库（Slave）**的方式连接 MySQL 主库，实现：
- **全量初始化同步**：基于 `mysqldump` 导出 + `mysql` 导入
- **增量实时同步**：解析 Binlog ROW 事件，写入目标库（支持 INSERT/UPDATE/DELETE）

与 Python 版 [mysql-sync](https://github.com/july2018/mysql-sync) 功能完全对齐。

---

## 为什么选择 Go 版？

| 特性 | Python 版 | Go 版 |
|------|-----------|-------|
| 部署 | 需安装 Python + 依赖 | **单二进制文件，零依赖** |
| 性能 | GIL 限制并发 | **goroutine 高并发** |
| 交叉编译 | 需额外工具 | **GOOS/GOARCH 一条命令** |
| 内存占用 | ~50MB+ | **~15MB** |
| 长期运行 | 需守护进程管理 | **原生适合服务化部署** |

---

## 项目结构

```
go_mysql_sync/
├── main.go                  # 主入口：参数解析、日志、调度
├── config/
│   └── config.go            # 配置结构体 + YAML 加载
├── fullsync/
│   └── fullsync.go          # 全量同步（mysqldump 管道）
├── incremental/
│   ├── position.go          # Binlog 位点持久化
│   ├── writer.go            # 目标库批量写入
│   └── stream.go            # Binlog 伪从库订阅（go-mysql canal）
├── checkenv/
│   └── checkenv.go          # 环境检查工具
├── config.example.yaml      # 配置示例
├── .gitignore
├── go.mod
└── go.sum
```

---

## 快速开始

### 1. 编译

```bash
# 本机编译
go build -o go_mysql_sync .

# 交叉编译 Linux 版
GOOS=linux GOARCH=amd64 go build -o go_mysql_sync .

# 交叉编译 Windows 版（在 Linux/Mac 上）
GOOS=windows GOARCH=amd64 go build -o go_mysql_sync.exe .
```

### 2. 配置

```bash
cp config.example.yaml config.yaml
# 编辑 config.yaml 填写源库和目标库信息
```

配置文件与 Python 版完全兼容，格式参考：

```yaml
source:
  host: "192.168.1.100"
  port: 3306
  user: "repl_user"
  password: "repl_password"

target:
  host: "192.168.1.200"
  port: 3306
  user: "sync_user"
  password: "sync_password"

sync:
  databases:
    - "mydb"
  incremental:
    server_id: 9999
```

### 3. 源库权限配置

```sql
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'repl_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

源库 `my.cnf`:

```ini
[mysqld]
server-id        = 1
log_bin          = /var/lib/mysql/mysql-bin.log
binlog_format    = ROW
binlog_row_image = FULL
```

### 4. 环境检查

```bash
./go_mysql_sync -config config.yaml -mode check
```

### 5. 运行

```bash
# 全量 + 增量（推荐首次使用）
./go_mysql_sync -config config.yaml -mode all

# 仅全量同步
./go_mysql_sync -config config.yaml -mode full

# 仅增量同步
./go_mysql_sync -config config.yaml -mode incremental

# 重置位点，从当前开始
./go_mysql_sync -config config.yaml -mode incremental -reset-position
```

---

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-config` | `config.yaml` | 配置文件路径 |
| `-mode` | `all` | 运行模式：`full` / `incremental` / `all` |
| `-reset-position` | `false` | 重置 Binlog 位点（谨慎使用） |
| `-version` | `false` | 显示版本信息 |

---

## 工作原理

### 全量同步流程

```
1. 查询源库 SHOW MASTER STATUS → 记录位点 (log_file, log_pos)
2. 并行执行 mysqldump --single-transaction → 管道 → mysql 导入
3. 将位点保存到 logs/binlog_position.json
```

### 增量同步流程

```
1. 读取 binlog_position.json 加载位点
2. go-mysql canal 伪装从库 (server_id=9999) 注册到主库
3. 接收 Binlog ROW 事件：
   canal.InsertAction → INSERT ... ON DUPLICATE KEY UPDATE
   canal.UpdateAction → UPDATE ... WHERE <主键>
   canal.DeleteAction → DELETE ... WHERE <主键>
4. channel 缓冲 → 批量写入目标库（事务提交）
5. 定时保存位点（断点续传）
```

### 关键特性

| 特性 | 实现方式 |
|------|----------|
| **断点续传** | 位点持久化到 JSON 文件，原子写入（tmp + rename） |
| **自动重连** | go-mysql canal 内建 + 目标库 Ping 重连 |
| **批量写入** | channel 缓冲 + 批次大小/超时双触发 |
| **幂等 INSERT** | `ON DUPLICATE KEY UPDATE` |
| **主键缓存** | 查询 information_schema 后内存缓存，避免重复查询 |
| **并发全量** | goroutine + 信号量控制并行数 |
| **信号处理** | SIGINT/SIGTERM 优雅退出 |

---

## 技术栈

| 库 | 用途 |
|----|------|
| [go-mysql-org/go-mysql](https://github.com/go-mysql-org/go-mysql) | Binlog 解析（canal 模式） |
| [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) | MySQL 驱动 |
| [yaml.v3](https://gopkg.in/yaml.v3) | YAML 配置解析 |
| [lumberjack.v2](https://gopkg.in/natefinsh/lumberjack.v2) | 日志滚动 |
| log/slog | 结构化日志（Go 1.21+ 标准库） |

---

## 与 Python 版对比

| 维度 | Python 版 | Go 版 |
|------|-----------|-------|
| Binlog 库 | python-mysql-replication | go-mysql canal |
| 配置格式 | 完全相同 | 完全相同 |
| 位点文件 | 完全兼容 | 完全兼容（同一 JSON 格式） |
| 全量方式 | subprocess 管道 | exec.Command 管道 |
| 并发 | ThreadPoolExecutor | goroutine + channel |
| 部署 | pip install + 3 个文件 | **单二进制文件** |

> 两个版本可以**共享同一个 config.yaml 和 binlog_position.json**，互相切换无需重新全量同步。

---

## 常见问题

**Q: 提示 `找不到 mysqldump`？**
A: 在 `config.yaml` 的 `full_sync.mysqldump_bin` 指定完整路径：
```yaml
mysqldump_bin: "/usr/bin/mysqldump"
```

**Q: 增量同步报 Binlog 文件不存在？**
A: 位点记录的 Binlog 已被清理。解决：
```bash
rm logs/binlog_position.json
./go_mysql_sync -mode all
```

**Q: 如何部署为 systemd 服务？**
```ini
[Unit]
Description=MySQL Sync
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/go_mysql_sync -config /etc/mysql-sync/config.yaml -mode all
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

---

## 源库 my.cnf 完整推荐配置

```ini
[mysqld]
server-id               = 1
log_bin                 = /var/lib/mysql/mysql-bin.log
binlog_format           = ROW
binlog_row_image        = FULL
expire_logs_days        = 7
max_binlog_size         = 100M
sync_binlog             = 1
innodb_flush_log_at_trx_commit = 1
```

---

## License

MIT
