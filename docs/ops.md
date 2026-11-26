# 运维手册 (leaf)

这台机器上跑着 A 股与台股的**全部无人值守任务**。这份文件只回答两个问题：
**什么时间跑什么**，以及**坏了先看哪里**。

时区：leaf 是 UTC。下表的时间都是 **UTC**，括号里是台北时间。

## 一、定时任务

| 时间 | 任务 | 谁拉起 | 失败会怎样 |
|---|---|---|---|
| `* 0-5` (08:00-13:59) | 台股盘中采集 | crontab 每分钟 `record_tw_quotes.sh` | 连续 5 轮失败发飞书；13:36 自动收工 |
| `05:40` (13:40) | 台股日更 (candles + 身份表) | `daily_tw.sh` | 发飞书，退出码非 0 |
| `06:00` (14:00) | 盘中快照 -> 1 分钟 bar | `build_tw_intraday_bars.py` | 与采集器无关；失败看 `intraday_bars.log` |
| `06:30` (14:30) | 备份 (盘中归档 / 因子 / 留痕) | `daily_backup.sh` | 发飞书 |
| `01:35` (09:35) | A 股一轮 (mock 下单) | `run_daily.sh` | 发飞书；退出码 1 表示有单没被受理 |
| `01:40` (09:40) | 台股一轮 (mock 下单) | `run_daily_tw.sh` | 同上 |
| `02:30` (10:30) | A 股补单轮 | `retry_rejected.sh cn` | 只补被拒的那几笔；没得补就静默退出 |
| `02:32` (10:32) | 台股补单轮 | `retry_rejected.sh tw` | 同上 |
| `08:43` (16:43) | A 股日更 | `daily.sh` | 发飞书 |
| `18:00` (次日 02:00) | 台股分红数据 (复权因子) | `daily_tw_dividends.sh` | 发飞书；抓完才重建 |
| `*/2` | 隧道看护 (A 股 / 台股) | `tunnel_vps.sh` / `watch_broker_tunnel.sh` | 只在 `curl` 超时 28 时重连 |
| `@reboot` | spider / free-stockdb / 隧道 | crontab | — |
| 常驻 | 交易看板 `market-board.service` | systemd (`Restart=always`) | `systemctl status market-board`，日志在 journald |

**时间为什么这么排**：日更（05:40）与分红（18:00）共用 FinMind 的 600/小时额度，
两段不重叠；盘中的采集与两轮下单都在交易时段内；备份在 bar 重建之后。

## 二、坏了先看哪里

| 症状 | 先看 | 常见原因 |
|---|---|---|
| A 股一轮报"环境不可用 / 503" | `stock_broker_a_server/log/server.err.log` | 同花顺没开、辅助功能没授权、栈溢出（已修） |
| 台股一轮报"unavailable" | 服务端 `/health` 的 `login_status` | 券商会话掉了 —— **重启服务端**，重试无用 |
| 台股一轮报"历史库没有 adj_factor" | `~/data/market/tw_dividends.log` | 分红数据没抓完 / 重建没跑 |
| 带 window 的因子被拒 | `adj_factor` 的 `max(date)` | 复权因子落后于 `candles`（见下面「复权因子」） |
| 采集器没数据 | `~/data/market/intraday_tw.log` | 隧道断、券商服务端挂了、非交易日 |
| 主机连不上 | 跳板 | `ssh jas@127.0.0.1 -p 6000` 走 xiaoxin 的本地隧道，比 `47.92.150.181:2222` 稳 |
| 日更报"源不可达 / 退出码 3"、7899 拒绝连接 | `pgrep stockdb`、`ss -lntp \| grep 7899` | 源进程被 OOM 杀掉（见「二之二」） |

**先跑预检**：它把上面一半的检查合成三行，而且**不碰钱**。

```
cd ~/apps/factor_maker
set -a; . ~/.config/factor_maker/env; set +a        # A 股
.venv/bin/factor-maker --preflight

set -a; . ~/.config/factor_maker/tw.env; set +a     # 台股
.venv/bin/factor-maker --market tw --config ~/.config/factor_maker/strategies.tw.toml --preflight
```

## 二之二、源服务 `http://127.0.0.1:7899`（stockdb）

日更、复权因子、代码表**全部**走这一个进程。它把 LevelDB 的数据读进内存再回答查询。

**2026-10-23 事故**：一个"取某 ETF 一整年分钟 K"的探针请求让它分配了 **15.3 GB**，
09:23:31 被 OOM killer 杀掉。随后磁盘 IO 排队（`/proc/pressure/io` 的 300 秒均值
96%），load 冲到 110，ssh 只能握完 TCP、完不成握手，看板和日更一起超时。
**内存释放后机器自己恢复，不需要重启。**

**规则（这次学到的）**：

1. 任何手工查询必须**有界**：单标的 + 单日，或者带 `num=` 限条数。
   `k1="all:"` + 前缀、跨年的时间窗，都属于"可能返回几百 MB"的写法。
2. **探针不许在 leaf 上跑。** 它和日更、看板共用这台机器；一次无界请求就是一次
   十几 GB 的分配。
3. `分钟k` 尤其危险：单标的全年约 6 万条，全市场一年是几十 GB 的量级。
4. 取分钟数据用于研究，应该走"单日 × 全市场"或者"单标的 × 单月"的分块下载，
   落到独立的 DuckDB，而不是每次从源上现查。

**恢复**：

```bash
bash /home/jas/apps/free_stockdb/start.sh   # 会清理 OOM 留下的 stale pidfile
ss -lntp | grep 7899                        # 等它监听
```

被 OOM 杀过之后，正在跑的 `daily.sh` 会以"源不可达（退出码 3）"失败。重启源之后
重跑 `/home/jas/apps/market_data/daily.sh` 即可（写入是幂等的）。

## 三、复权因子

台股的 `adj_factor` 是**生产出来的**，不是日更的副产物：

1. `daily_tw_dividends.sh` 按标的抓分红（FinMind，2,770 只，约 5 小时，分几晚）；
2. 全部抓完才重建：备份 -> **清空** -> `import-adj-factors` -> 校验 `max(date)` 对齐。

抓完之后进入**日常维护**，不再是每晚扫 2,770 只：

- **增量**：每天从 TWSE 的 `TWT48U_ALL`（未来一个月的除权息日程）取"最近 14 天除过息"的标的，
  只重抓这几只（通常个位数）。FinMind 的结果数据在除息日之后才有；
- **月度全量**：每 30 天换一份 `--end`，所有标的重抓一遍，兜住增量漏掉的（新上市、job 停过几天）。

两个必须记住的点：

- **重建前必须清空**。`import-adj-factors` 是 `INSERT OR IGNORE`，旧行盖不掉，
  新旧两套因子会按日期拼在一起。
- **`import-adj-factors` 不读 `STOCK_TW_DB_PATH`**，它只认 `--db-path`
  （默认就是生产库）。2026-10-22 在副本上验证时因此写进了生产库。
- **leaf 解析不了 `openapi.twse.com.tw`**：DNS 把它解到连不通的地址段（实测是 Facebook 的 IP，
  连接被拒），而 FinMind 正常。脚本改用阿里 DoH 拿候选 IP、再 `curl --resolve` 逐个试；
  DoH 给的地址里也有一半连不通（185.45.x / 124.11.x 超时），所以还带两个 Hinet CDN 地址兜底
  （203.74.221.3 / 203.74.220.1）。VPS 上的 leaf 密钥是 `command="/bin/false"` 的受限密钥，
  不能拿来跑命令 —— 别为了取个公告去放宽它。

## 四、数据资产

| 数据 | 可再生? | 备份 |
|---|---|---|
| `market_v2.duckdb` (candles/adj_factor) | 是，但要 6-11 小时 | adj_factor 每日 parquet (7 天) |
| `market_tw_intraday.duckdb` | **否** —— 券商只给实时快照 | 按天 parquet，永久 |
| `factor_maker/journal.db` | **否** —— 审计 | 每日拷贝 (30 天) |

归档在 `~/data/backup/`，恢复就是一条 `INSERT ... SELECT * FROM read_parquet(...)`。

## 四之二、交易看板

`market-board.service` 监听 leaf 的 `127.0.0.1:8500`，**只读**：留痕 / 持仓 / 订单 / 行情 / 数据新鲜度。

```bash
ssh -L 8500:127.0.0.1:8500 jas@127.0.0.1 -p 6000   # 然后浏览器开 http://127.0.0.1:8500
```

部署：`market_board/deploy/sync.sh`（前端在 Mac 构建；leaf 没有 node）。
环境文件：`~/.config/market_board/env`（600）。

**它会刻意避开交易链路**：A 股的持仓/订单接口会驱动同花顺面板，所以总览只打
`/health`，持仓与订单页不自动刷新，后端 60 秒缓存。

**台股日更曾经整轮独占 `market_v2`（2026-10-22 已修）**：`stock_tw fetch-history`
整个进程只开一个 DuckDB 连接（6-11 小时），而 DuckDB 的写锁是**连接级**的，于是看板与
研究脚本在下午/晚上读不到台股行情（`DB_LOCKED` / `Conflicting lock`）。

修法（`stock_tw`，那份代码在 leaf 上没有版本控制，改前已备份为 `*.bak-20261022061001`）：

- `MarketDataRepository` 记住库路径，新增 `release()`（关掉并置空）与惰性重连的 `_live`；
- `BackfillService._fetch_symbol_history` 在两处释放连接：**读完 resume_start 之后**
  （接下来是几秒的限流等待）和**写完之后**。窗口就是那几秒；
- `reopen_every=200` 作为兜底：即使没有限流等待，每 200 只也放一次。

实测：抓取进行中，另一个进程连续 111 次只读全部成功。

## 四之三、告警用的 `lark_alert` 扩展

`lark_alert` 是 workspace 里的 Rust crate，带 **PyO3 绑定**（`LarkAlert` / `Card` / `Severity`）。
用它意味着 Python 侧与两个 Rust 服务端**同一份实现**（重试、签名、卡片格式）。

它不在 PyPI 上，要在目标机上现编。leaf 上已经编好并装进 `factor_maker` 与
`market_board` 两个 venv：

```bash
rsync -az --exclude target --exclude .git --exclude dist lark_alert/ leaf:~/apps/lark_alert/
# leaf 上（Rust 工具链只需装一次）:
curl https://sh.rustup.rs | sh -s -- -y --profile minimal
cd ~/apps/lark_alert
uv run --with maturin maturin build --release -i ~/apps/factor_maker/.venv/bin/python
uv pip install target/wheels/lark_alert-*.whl      # 每个要用它的 venv 各装一次
```

**没有扩展的机器**（开发用的 Mac）会自动退回一条 `urllib` POST：返回值一致，
只是没有重试与卡片格式。两条路都有测试覆盖。

## 四之四、币圈采集与那份会骗人的 daily slim

链路：VPS (`/root/market_crypto`) 01:15 UTC 采集 → 01:45 导出 7 天增量 parquet →
leaf 02:00 通过隧道拉取并合并进 `~/data/research/market_crypto.duckdb`。

**leaf 上有两个库，只有一个在更新**：

| 文件 | 谁在写 | 用途 |
|---|---|---|
| `market_crypto.duckdb` | leaf 上 `collect_crypto.py` 每天 02:00 UTC，经 xiaoxin 代理 | 研究 + 策略 + 看板（**以它为准**） |
| `market_crypto_daily.duckdb` | **没有任何任务** | 2026-10-26 为一次大传输失败切出的日线副本 |

2026-10-27 踩到的坑：live 与看板都指向那份 daily slim，而它停在 10-26 09:56 ——
当天的日线 bar 是**十小时前的半根**，"最后一根已收盘 bar"的判断按时间算成立，
于是连跑四轮调仓，用的都是不存在的收盘价。现在两边都改指全量库；`live.py` 另加
一条：最后一根已收盘日线离现在超过 **3 天**就拒绝跑（拿旧 bar 算信号不会报错，
只会安静地按几天前的行情调仓）。

**采集范围**：`--top 0` = 全部 USDT **PERPETUAL**（523 只）。`TRADIFI_PERPETUAL`
（AAPL/TSLA 这类代币股票）与交割合约都不采 —— 主网上有 229 只代币股票，占了采集池
的三分之一，走势跟着美股走，不是这个策略要交易的东西。

**增量**：每档从"库里最后一根"开始重拉（不是 +1），`ON CONFLICT DO UPDATE`
修正还没收的半根 bar。全量重拉 523 只要 7 小时，增量十几分钟 —— 日更能不能每天跑
就取决这一条。

**403 是按住连接挡的**（2026-10-27）：回补进程在 400 次请求后开始 403（纯 nginx
页面，不是 JSON 错误），退避 30/60/90 秒都没用；同时另开一个进程请求**同一个
URL** 却是 200。原因是 httpx 复用连接池，会一直粘在被挡的那个边缘节点上。
解法是 403 时 `close()` 掉连接池重建 —— 现在这么做了。

**扩展标的池后**要补一次全量历史（日常增量只带窗口内数据，新标的做不了带窗口的研究）。2026-11-12 起 VPS 关机，采集直接在 leaf 上跑，通过 xiaoxin 的 mihomo 出网：

```bash
cd ~/apps/market_data
HTTPS_PROXY=http://192.168.0.108:7890 HTTP_PROXY=http://192.168.0.108:7890 \
  .venv/bin/python scripts/collect_crypto.py \
  --db ~/data/research/market_crypto.duckdb --symbols <new-symbol> --days-1m 90
```

## 四之五、台股日线: 交易所单日全市场

2026-10-27 起，日线不再走 FinMind 的逐标的日更。原因有两条，都是结构性的：

1. **慢**：3,134 只 × 1 请求，540 请求/小时 → 每天 6-11 小时；
2. **晚**：05:40 UTC 开跑时 FinMind 还没有当天数据（台股 13:30 收盘，通常要几小时后才发布），于是每天稳定落后一天，而且**经常只更新了一部分标的**——10-27 实测 2,831 只里只写进去 943 只，策略会以为自己看到了全市场。

现在的链路（2026-11-12 起；VPS 关机后改由 xiaoxin 抓取）：

```
xiaoxin 16:50/17:30 CST  tw_daily_openapi.py（台湾代理）
       ├─ TWSE afterTrading/STOCK_DAY_ALL?date=YYYYMMDD   (CSV, 上市)
       └─ TPEX openapi/v1/tpex_mainboard_quotes            (JSON, 上柜)
     → /home/jas/data/market/tw_daily_recent.parquet
     → rsync 到 leaf
leaf 17:00/17:40 CST  scripts/sync_tw_daily.sh
       ├─ upsert candles (symbol,date)
       └─ adj_factor 顺延: 新日期沿用该标的最近一天的因子
```

TPEX 当天数据有时晚于 TWSE，17:30 那次抓取和 leaf 17:40 的重试就是为此准备的；
`check_freshness.py` 还会检查"最新一天是不是只有半个市场"。

**adj_factor 必须和 candles 一起推进**：dataset 加载是 INNER JOIN，没有复权因子的
bar 会被整根丢掉——candles 更新了而 adj 没跟上，等于没更新（10-27 上午就是这样：
candles 到 10-27、adj 停在 10-22）。除息当天的修正仍由 18:00 的
`daily_tw_dividends.sh` 作废重建。

**覆盖差异**：交易所单日接口不含**兴柜 (ESB)**（约 400 只）。生产不交易兴柜
（`universe_markets = TWSE, TPEX`），研究若要用兴柜，历史仍在库里，只是不再逐日更新。

FinMind 那条改成**每周六**（`daily_tw.sh`）：兴柜、分红原始数据、身份表还靠它。

## 五、账户

- `factor_maker`：策略的单，mock。
- `default`：ex_monitor 的模拟单（它显式带 `mock: true`）。**模拟账户的持仓与
  实盘不是同一份**，按实盘持仓算出来的卖出量在模拟账户上可能不够。
- 真实单（`mock: false`）会走同花顺；风控的参考价来自最近一次账户快照。
