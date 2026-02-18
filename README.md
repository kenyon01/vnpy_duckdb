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
