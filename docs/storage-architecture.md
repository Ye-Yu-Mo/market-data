# 数据存储：在线 psql，离线 parquet

2026-11-16 决定。触发原因：单文件 DuckDB 是**单写者**，一次日更/回补就把所有
读者挡在外面（11-12 台股卡死 44 小时、11-16 A股日更挡住看板和回测，都是这个）。

## 分工

| 层 | 存储 | 放什么 |
|---|---|---|
| 在线 | PostgreSQL (`stock_a_spider`) | 当日/近期分钟线、实时快照、看板要的低延迟查询 |
| 离线 | parquet（Hive 分区） | 日线、历史分钟线、因子面板、信号、权益快照 |

parquet 只读、多进程并发无锁；psql 负责并发读写。

## 离线布局（分标的、分日期）

```text
/home/jas/data/archive/
  bars/cn/1d/symbol=600519/year=2026/data_0.parquet      # 一张表 = 一只标的一年的历史
  bars/cn/1d_by_date/date=2026-11-13/data_0.parquet      # 一张表 = 某天全市场截面
  bars/tw/1d/symbol=2330/year=2026/data_0.parquet
  bars/tw/1d_by_date/date=2026-11-16/data_0.parquet
  bars/tw/1m/symbol=2330/trade_date=2026-11-16/data_0.parquet
  flows/tw/symbol=2330/year=2026/data_0.parquet
  revenue/tw/symbol=2330/revenue.parquet
  factors/cn/lower_shadow/symbol=600519/year=2026/data_0.parquet
  factors/cn/amplitude/symbol=600519/year=2026/data_0.parquet
  signals/event/bq10/date=2026-11-16/data_0.parquet
  signals/factor_maker/dip_rotation_cn/date=2026-11-16/data_0.parquet
  equity/cn/factor_maker/year=2026/data_0.parquet
  symbols/cn.parquet   symbols/tw.parquet
```

规则：

- **日线按 symbol + year**：一只标的 = 一个目录，一年一个文件；当年文件每天重写，
  往年不可变。
- **同一份日线再按 date 存一份**：某一天的截面查询只开一天的文件。
  只有 symbol 布局时，2026 年截面要打开 8,000 个文件（实测 34s）；date 布局下
  是一个目录（0.05s）。用存储换查询速度，值得。
- **分钟线按 symbol + date**：一天一个文件，写完即不可变，天然无锁。
- **因子/信号也按 symbol 或 date**：因子跟日线同构；信号是事件流，按 date 分区，
  行里带 symbol。
- 所有写入用 `COPY ... (FORMAT PARQUET, PARTITION_BY (...), OVERWRITE_OR_IGNORE)`，
  原子替换，不在原地改。

查询示例：

```sql
-- 一只标的的全部历史
select * from read_parquet('/home/jas/data/archive/bars/tw/1d/symbol=2330/**/*.parquet') order by date;
-- 某一天的全市场截面
select * from read_parquet('/home/jas/data/archive/bars/tw/1d/symbol=*/year=2026/*.parquet') where date = '2026-11-16';
-- 某因子某年的截面
select * from read_parquet('/home/jas/data/archive/factors/cn/amplitude/symbol=*/year=2026/*.parquet');
```

## 因子与信号归档

- **因子**：`factors/<market>/<factor>/symbol=XXXX/year=YYYY/data.parquet`，
  列固定为 `symbol, date, value, horizon, window`。每天收盘后为当日全市场追加，
  历史由研究脚本一次性回填。
- **信号**：`signals/<source>/date=YYYY-MM-DD/data.parquet`。
  - 事件合约：把 `ec_signals_*.jsonl` 按日期切分（`line, ts, symbol, side, prob, ...`）；
  - factor_maker：每轮目标的 `run_id, strategy, symbol, target_weight, side` 与
    委托 `client_order_id, status, fill_price`；
  - 轮动：`latest.json` 的 picks 与 history。
- **权益**：`equity/<market>/<account>/year=YYYY/data.parquet`。

原则同行情：**一份数据只能有一个写者**，写完即不可变；按天/按年重写当前分区。

## 每日导出与清理

在线库不能无限长。每天收盘后：

```text
14:00  build_tw_intraday_bars.py --psql-dsn ... --no-duckdb  # psql quotes -> klines(tw,1m)
17:45  archive_bars.py --since-year 2026     # 日线增量（只重写当年/当天分区）
17:47  archive_bars.py --what intraday --since-date today  # 台股 1m 增量（读 psql）
17:50  archive_daily.py --clean               # 导出当天 ticks/1m/5m, 再按保留期清理
17:52  archive_signals.py                     # factor_maker runs + 轮动
```

台股盘中链路（2026-11-17 起）：

```text
采集器 -> psql quote_samples（每 15 秒一帧, PK: market,code,ts,recv_ms）
14:00  -> psql klines (tw, 1m)      看板 / 归档 / 研究都读这一份
17:47  -> parquet bars/tw/1m/symbol=.../trade_date=...
```

`market_tw_intraday.duckdb` 在 11-17 对齐验证后退役：DuckDB 与 psql 的 17 个交易日
逐值一致（条数、OHLC、成交量，max diff = 0）。

事件信号历史（xiaoxin）：`0 9 * * *` UTC 跑 `ec_archive_signals.py`，写到
`signals/event/{bq10,bq}/date=...`，由每分钟的 `sync_to_leaf.sh` 推到 leaf。

保留期（`archive_daily.py` 里的 TABLES）：

| 表 | 保留 | 说明 |
|---|---:|---|
| `ticks` | 10 天 | 逐笔，涨得最快（约 40 万行/天） |
| `klines 1m` | 30 天 | 看板分钟线要用的窗口 |
| `klines 5m` | 90 天 | |
| `klines 1d` | 不删 | 量小 |

**清理顺序是硬的：先确认某天的 parquet 分区存在，才允许删那一天。** 归档缺失时
跳过并记日志，绝不在没有备份的情况下删在线数据。

## 迁移顺序

1. **归档回填**（不改变现有读写）：`archive_bars.py` 把日线/分钟线导出到 parquet；
2. **读者切换**：看板 `marketdb`、factor_maker 的 feed、研究脚本改读 parquet；
3. **写者切换**：日更直接写 parquet（按 symbol/year 原子替换）；盘中采集写 psql；
4. **因子/信号归档**：研究侧产出落 parquet，信号落 date 分区；
5. 旧的 `market_a.duckdb` / `market_v2.duckdb` 保留为只读快照，确认无人使用后删除。

## 脚本

- `market_data/scripts/archive_bars.py` —— 日线/分钟线归档（已实现）。
  `--what daily|intraday --market cn|tw [--since-year N] [--since-date D]`