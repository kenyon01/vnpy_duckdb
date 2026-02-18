from datetime import datetime
from pathlib import Path

import duckdb

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    TickOverview,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


DB_PATH: str = SETTINGS["database.database"]


class DuckdbDatabase(BaseDatabase):
    """DuckDB数据库接口"""

    def __init__(self) -> None:
        """"""
        path: Path = Path(DB_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(str(path))
        self._create_tables()

    def _create_tables(self) -> None:
        """创建数据表"""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bar_data (
                symbol        VARCHAR   NOT NULL,
                exchange      VARCHAR   NOT NULL,
                datetime      TIMESTAMP NOT NULL,
                interval      VARCHAR   NOT NULL,
                volume        DOUBLE    NOT NULL DEFAULT 0,
                turnover      DOUBLE    NOT NULL DEFAULT 0,
                open_interest DOUBLE    NOT NULL DEFAULT 0,
                open_price    DOUBLE    NOT NULL DEFAULT 0,
                high_price    DOUBLE    NOT NULL DEFAULT 0,
                low_price     DOUBLE    NOT NULL DEFAULT 0,
                close_price   DOUBLE    NOT NULL DEFAULT 0,
                PRIMARY KEY (symbol, exchange, interval, datetime)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tick_data (
                symbol        VARCHAR   NOT NULL,
                exchange      VARCHAR   NOT NULL,
                datetime      TIMESTAMP NOT NULL,
                name          VARCHAR   NOT NULL DEFAULT '',
                volume        DOUBLE    NOT NULL DEFAULT 0,
                turnover      DOUBLE    NOT NULL DEFAULT 0,
                open_interest DOUBLE    NOT NULL DEFAULT 0,
                last_price    DOUBLE    NOT NULL DEFAULT 0,
                last_volume   DOUBLE    NOT NULL DEFAULT 0,
                limit_up      DOUBLE    NOT NULL DEFAULT 0,
                limit_down    DOUBLE    NOT NULL DEFAULT 0,
                open_price    DOUBLE    NOT NULL DEFAULT 0,
                high_price    DOUBLE    NOT NULL DEFAULT 0,
                low_price     DOUBLE    NOT NULL DEFAULT 0,
                pre_close     DOUBLE    NOT NULL DEFAULT 0,
                bid_price_1   DOUBLE    NOT NULL DEFAULT 0,
                bid_price_2   DOUBLE,
                bid_price_3   DOUBLE,
                bid_price_4   DOUBLE,
                bid_price_5   DOUBLE,
                ask_price_1   DOUBLE    NOT NULL DEFAULT 0,
                ask_price_2   DOUBLE,
                ask_price_3   DOUBLE,
                ask_price_4   DOUBLE,
                ask_price_5   DOUBLE,
                bid_volume_1  DOUBLE    NOT NULL DEFAULT 0,
                bid_volume_2  DOUBLE,
                bid_volume_3  DOUBLE,
                bid_volume_4  DOUBLE,
                bid_volume_5  DOUBLE,
                ask_volume_1  DOUBLE    NOT NULL DEFAULT 0,
                ask_volume_2  DOUBLE,
                ask_volume_3  DOUBLE,
                ask_volume_4  DOUBLE,
                ask_volume_5  DOUBLE,
                localtime     TIMESTAMP,
                PRIMARY KEY (symbol, exchange, datetime)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bar_overview (
                symbol   VARCHAR   NOT NULL,
                exchange VARCHAR   NOT NULL,
                interval VARCHAR   NOT NULL,
                count    INTEGER   NOT NULL DEFAULT 0,
                start_dt TIMESTAMP NOT NULL,
                end_dt   TIMESTAMP NOT NULL,
                PRIMARY KEY (symbol, exchange, interval)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tick_overview (
                symbol   VARCHAR   NOT NULL,
                exchange VARCHAR   NOT NULL,
                count    INTEGER   NOT NULL DEFAULT 0,
                start_dt TIMESTAMP NOT NULL,
                end_dt   TIMESTAMP NOT NULL,
                PRIMARY KEY (symbol, exchange)
            )
        """)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存K线数据"""
        # 读取主键参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval = bar.interval

        # 将BarData数据转换为元组列表，并调整时区
        data: list[tuple] = []
        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)
            data.append((
                bar.symbol,
                bar.exchange.value,
                bar.datetime,
                bar.interval.value,
                bar.volume,
                bar.turnover,
                bar.open_interest,
                bar.open_price,
                bar.high_price,
                bar.low_price,
                bar.close_price,
            ))

        # 使用upsert操作将数据批量写入数据库
        self.conn.executemany("""
            INSERT INTO bar_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, exchange, interval, datetime) DO UPDATE SET
                volume        = excluded.volume,
                turnover      = excluded.turnover,
                open_interest = excluded.open_interest,
                open_price    = excluded.open_price,
                high_price    = excluded.high_price,
                low_price     = excluded.low_price,
                close_price   = excluded.close_price
        """, data)

        # 更新K线汇总数据
        row: tuple | None = self.conn.execute("""
            SELECT count, start_dt, end_dt FROM bar_overview
            WHERE symbol = ? AND exchange = ? AND interval = ?
        """, [symbol, exchange.value, interval.value]).fetchone()

        if not row:
            self.conn.execute("""
                INSERT INTO bar_overview VALUES (?, ?, ?, ?, ?, ?)
            """, [
                symbol, exchange.value, interval.value,
                len(bars), bars[0].datetime, bars[-1].datetime
            ])
        elif stream:
            self.conn.execute("""
                UPDATE bar_overview
                SET end_dt = ?, count = count + ?
                WHERE symbol = ? AND exchange = ? AND interval = ?
            """, [bars[-1].datetime, len(bars), symbol, exchange.value, interval.value])
        else:
            count: int = self.conn.execute("""
                SELECT COUNT(*) FROM bar_data
                WHERE symbol = ? AND exchange = ? AND interval = ?
            """, [symbol, exchange.value, interval.value]).fetchone()[0]

            new_start: datetime = min(bars[0].datetime, row[1])
            new_end: datetime = max(bars[-1].datetime, row[2])

            self.conn.execute("""
                UPDATE bar_overview
                SET start_dt = ?, end_dt = ?, count = ?
                WHERE symbol = ? AND exchange = ? AND interval = ?
            """, [new_start, new_end, count, symbol, exchange.value, interval.value])

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        # 读取主键参数
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        # 将TickData数据转换为元组列表，并调整时区
        data: list[tuple] = []
        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)
            data.append((
                tick.symbol,
                tick.exchange.value,
                tick.datetime,
                tick.name,
                tick.volume,
                tick.turnover,
                tick.open_interest,
                tick.last_price,
                tick.last_volume,
                tick.limit_up,
                tick.limit_down,
                tick.open_price,
                tick.high_price,
                tick.low_price,
                tick.pre_close,
                tick.bid_price_1,
                tick.bid_price_2,
                tick.bid_price_3,
                tick.bid_price_4,
                tick.bid_price_5,
                tick.ask_price_1,
                tick.ask_price_2,
                tick.ask_price_3,
                tick.ask_price_4,
                tick.ask_price_5,
                tick.bid_volume_1,
                tick.bid_volume_2,
                tick.bid_volume_3,
                tick.bid_volume_4,
                tick.bid_volume_5,
                tick.ask_volume_1,
                tick.ask_volume_2,
                tick.ask_volume_3,
                tick.ask_volume_4,
                tick.ask_volume_5,
                tick.localtime,
            ))

        # 使用upsert操作将数据批量写入数据库
        self.conn.executemany("""
            INSERT INTO tick_data VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT (symbol, exchange, datetime) DO UPDATE SET
                name          = excluded.name,
                volume        = excluded.volume,
                turnover      = excluded.turnover,
                open_interest = excluded.open_interest,
                last_price    = excluded.last_price,
                last_volume   = excluded.last_volume,
                limit_up      = excluded.limit_up,
                limit_down    = excluded.limit_down,
                open_price    = excluded.open_price,
                high_price    = excluded.high_price,
                low_price     = excluded.low_price,
                pre_close     = excluded.pre_close,
                bid_price_1   = excluded.bid_price_1,
                bid_price_2   = excluded.bid_price_2,
                bid_price_3   = excluded.bid_price_3,
                bid_price_4   = excluded.bid_price_4,
                bid_price_5   = excluded.bid_price_5,
                ask_price_1   = excluded.ask_price_1,
                ask_price_2   = excluded.ask_price_2,
                ask_price_3   = excluded.ask_price_3,
                ask_price_4   = excluded.ask_price_4,
                ask_price_5   = excluded.ask_price_5,
                bid_volume_1  = excluded.bid_volume_1,
                bid_volume_2  = excluded.bid_volume_2,
                bid_volume_3  = excluded.bid_volume_3,
                bid_volume_4  = excluded.bid_volume_4,
                bid_volume_5  = excluded.bid_volume_5,
                ask_volume_1  = excluded.ask_volume_1,
                ask_volume_2  = excluded.ask_volume_2,
                ask_volume_3  = excluded.ask_volume_3,
                ask_volume_4  = excluded.ask_volume_4,
                ask_volume_5  = excluded.ask_volume_5,
                localtime     = excluded.localtime
        """, data)

        # 更新Tick汇总数据
        row: tuple | None = self.conn.execute("""
            SELECT count, start_dt, end_dt FROM tick_overview
            WHERE symbol = ? AND exchange = ?
        """, [symbol, exchange.value]).fetchone()

        if not row:
            self.conn.execute("""
                INSERT INTO tick_overview VALUES (?, ?, ?, ?, ?)
            """, [symbol, exchange.value, len(ticks), ticks[0].datetime, ticks[-1].datetime])
        elif stream:
            self.conn.execute("""
                UPDATE tick_overview
                SET end_dt = ?, count = count + ?
                WHERE symbol = ? AND exchange = ?
            """, [ticks[-1].datetime, len(ticks), symbol, exchange.value])
        else:
            count: int = self.conn.execute("""
                SELECT COUNT(*) FROM tick_data
                WHERE symbol = ? AND exchange = ?
            """, [symbol, exchange.value]).fetchone()[0]

            new_start: datetime = min(ticks[0].datetime, row[1])
            new_end: datetime = max(ticks[-1].datetime, row[2])

            self.conn.execute("""
                UPDATE tick_overview
                SET start_dt = ?, end_dt = ?, count = ?
                WHERE symbol = ? AND exchange = ?
            """, [new_start, new_end, count, symbol, exchange.value])

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        rows: list[tuple] = self.conn.execute("""
            SELECT symbol, exchange, datetime, interval,
                   volume, turnover, open_interest,
                   open_price, high_price, low_price, close_price
            FROM bar_data
            WHERE symbol = ? AND exchange = ? AND interval = ?
              AND datetime >= ? AND datetime <= ?
            ORDER BY datetime
        """, [symbol, exchange.value, interval.value, start, end]).fetchall()

        bars: list[BarData] = []
        for row in rows:
            bar: BarData = BarData(
                symbol=row[0],
                exchange=Exchange(row[1]),
                datetime=datetime.fromtimestamp(row[2].timestamp(), DB_TZ),
                interval=Interval(row[3]),
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                open_price=row[7],
                high_price=row[8],
                low_price=row[9],
                close_price=row[10],
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取TICK数据"""
        rows: list[tuple] = self.conn.execute("""
            SELECT symbol, exchange, datetime, name,
                   volume, turnover, open_interest,
                   last_price, last_volume, limit_up, limit_down,
                   open_price, high_price, low_price, pre_close,
                   bid_price_1, bid_price_2, bid_price_3, bid_price_4, bid_price_5,
                   ask_price_1, ask_price_2, ask_price_3, ask_price_4, ask_price_5,
                   bid_volume_1, bid_volume_2, bid_volume_3, bid_volume_4, bid_volume_5,
                   ask_volume_1, ask_volume_2, ask_volume_3, ask_volume_4, ask_volume_5,
                   localtime
            FROM tick_data
            WHERE symbol = ? AND exchange = ?
              AND datetime >= ? AND datetime <= ?
            ORDER BY datetime
        """, [symbol, exchange.value, start, end]).fetchall()

        ticks: list[TickData] = []
        for row in rows:
            tick: TickData = TickData(
                symbol=row[0],
                exchange=Exchange(row[1]),
                datetime=datetime.fromtimestamp(row[2].timestamp(), DB_TZ),
                name=row[3],
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                last_price=row[7],
                last_volume=row[8],
                limit_up=row[9],
                limit_down=row[10],
                open_price=row[11],
                high_price=row[12],
                low_price=row[13],
                pre_close=row[14],
                bid_price_1=row[15],
                bid_price_2=row[16],
                bid_price_3=row[17],
                bid_price_4=row[18],
                bid_price_5=row[19],
                ask_price_1=row[20],
                ask_price_2=row[21],
                ask_price_3=row[22],
                ask_price_4=row[23],
                ask_price_5=row[24],
                bid_volume_1=row[25],
                bid_volume_2=row[26],
                bid_volume_3=row[27],
                bid_volume_4=row[28],
                bid_volume_5=row[29],
                ask_volume_1=row[30],
                ask_volume_2=row[31],
                ask_volume_3=row[32],
                ask_volume_4=row[33],
                ask_volume_5=row[34],
                localtime=row[35],
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        count: int = self.conn.execute("""
            SELECT COUNT(*) FROM bar_data
            WHERE symbol = ? AND exchange = ? AND interval = ?
        """, [symbol, exchange.value, interval.value]).fetchone()[0]

        self.conn.execute("""
            DELETE FROM bar_data
            WHERE symbol = ? AND exchange = ? AND interval = ?
        """, [symbol, exchange.value, interval.value])

        self.conn.execute("""
            DELETE FROM bar_overview
            WHERE symbol = ? AND exchange = ? AND interval = ?
        """, [symbol, exchange.value, interval.value])

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        count: int = self.conn.execute("""
            SELECT COUNT(*) FROM tick_data
            WHERE symbol = ? AND exchange = ?
        """, [symbol, exchange.value]).fetchone()[0]

        self.conn.execute("""
            DELETE FROM tick_data
            WHERE symbol = ? AND exchange = ?
        """, [symbol, exchange.value])

        self.conn.execute("""
            DELETE FROM tick_overview
            WHERE symbol = ? AND exchange = ?
        """, [symbol, exchange.value])

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """查询数据库中的K线汇总信息"""
        # 如果已有K线数据但缺失汇总信息，则执行初始化
        data_count: int = self.conn.execute(
            "SELECT COUNT(*) FROM bar_data"
        ).fetchone()[0]
        overview_count: int = self.conn.execute(
            "SELECT COUNT(*) FROM bar_overview"
        ).fetchone()[0]
        if data_count and not overview_count:
            self.init_bar_overview()

        rows: list[tuple] = self.conn.execute("""
            SELECT symbol, exchange, interval, count, start_dt, end_dt
            FROM bar_overview
        """).fetchall()

        overviews: list[BarOverview] = []
        for row in rows:
            overview: BarOverview = BarOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                interval=Interval(row[2]),
                count=row[3],
                start=datetime.fromtimestamp(row[4].timestamp(), DB_TZ),
                end=datetime.fromtimestamp(row[5].timestamp(), DB_TZ),
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """查询数据库中的Tick汇总信息"""
        rows: list[tuple] = self.conn.execute("""
            SELECT symbol, exchange, count, start_dt, end_dt
            FROM tick_overview
        """).fetchall()

        overviews: list[TickOverview] = []
        for row in rows:
            overview: TickOverview = TickOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                count=row[2],
                start=datetime.fromtimestamp(row[3].timestamp(), DB_TZ),
                end=datetime.fromtimestamp(row[4].timestamp(), DB_TZ),
            )
            overviews.append(overview)

        return overviews

    def init_bar_overview(self) -> None:
        """初始化数据库中的K线汇总信息"""
        rows: list[tuple] = self.conn.execute("""
            SELECT symbol, exchange, interval,
                   COUNT(*) AS cnt,
                   MIN(datetime) AS start_dt,
                   MAX(datetime) AS end_dt
            FROM bar_data
            GROUP BY symbol, exchange, interval
        """).fetchall()

        for row in rows:
            self.conn.execute("""
                INSERT INTO bar_overview VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, exchange, interval) DO UPDATE SET
                    count    = excluded.count,
                    start_dt = excluded.start_dt,
                    end_dt   = excluded.end_dt
            """, list(row))
