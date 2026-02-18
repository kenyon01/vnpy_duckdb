# vnpy_duckdb

[VeighNa](https://www.vnpy.com) 量化交易框架的 DuckDB 数据库驱动。

## 简介

`vnpy_duckdb` 将 [DuckDB](https://duckdb.org/) 嵌入式分析型数据库接入 VeighNa 框架的标准数据库接口（`BaseDatabase`），支持 K 线（Bar）和 Tick 行情数据的存储与查询。

DuckDB 相比 SQLite 具有更强的列式存储和批量查询性能，且无需独立服务进程，适合本地量化研究场景。

## 功能

- 保存 / 读取 K 线数据（`BarData`）
- 保存 / 读取 Tick 数据（`TickData`）
- 删除指定合约数据
- 查询数据汇总（`BarOverview` / `TickOverview`）
- 支持 upsert（重复写入自动更新，不产生重复行）
- **支持多进程并发读取**（CTA 策略参数优化时无文件锁冲突）

## 多进程支持说明

DuckDB 在 Windows 上不支持多进程同时以读写模式访问同一文件。本驱动通过以下方式解决该问题：

- **所有连接默认使用只读模式**，允许多个子进程（如参数优化的 `ProcessPoolExecutor`）并发读取
- **写操作**（数据导入/删除）通过临时读写连接完成，写完立即关闭，随后恢复只读模式
- **子进程初始化**跳过建表步骤（表由主进程在启动时创建），直接以只读模式连接

```
主进程启动   → 读写连接建表 → 关闭 → 只读连接 self.conn
数据导入时   → 临时读写连接 → 写入 → 关闭 → 恢复只读连接
优化子进程   → 跳过建表    → 只读连接（多进程并发安全）
```

> 注意：数据导入与多进程优化不应同时运行，否则临时读写连接会与子进程的只读连接冲突。

## 数据表结构

| 表名 | 主键 | 说明 |
|---|---|---|
| `bar_data` | symbol + exchange + interval + datetime | K 线数据 |
| `tick_data` | symbol + exchange + datetime | Tick 数据 |
| `bar_overview` | symbol + exchange + interval | K 线汇总信息 |
| `tick_overview` | symbol + exchange | Tick 汇总信息 |

> 注意：DuckDB 保留字 `start` / `end` 不可作为列名，汇总表改用 `start_dt` / `end_dt`。

## 安装

```bash
pip install vnpy_duckdb
```

或从源码安装：

```bash
git clone https://github.com/kenyon01/vnpy_duckdb.git
pip install -e vnpy_duckdb
```

## 配置

在 VeighNa 的 `setting.py` 或 `vt_setting.json` 中添加：

```json
{
    "database.name": "duckdb",
    "database.database": "D:/MarketData/vnpy_market.duckdb"
}
```

`database.database` 填写 `.duckdb` 文件的完整路径，文件不存在时会自动创建。

## 依赖

- Python >= 3.10
- duckdb >= 0.10.0
- vnpy >= 4.0.0

## 许可证

MIT License
