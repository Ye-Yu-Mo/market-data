# PLAN

## 背景

### 当前问题

**A 股没有可研究的日线历史。** 手里只有两条通路，都不成立：

| 通路 | 状态 |
|---|---|
| psql `klines`（leaf，`stock_a_spider` 落库） | 只有 **13 根日线**——那是 spider 的工作缓冲，不是历史库 |
| `free-stockdb`（leaf，23G） | 服务此前未恢复（二进制过期，2026-09-23 已修复） |

同时 **`market_data` 本身只是骨架**：515 行、7 个文件，已实现的是 `offline/` 三件套（manifest/quality/publish）+ alerts，`test_offline.py` 4 个测试通过。`markets/`、`offline/pipeline.py`、`update/sync/recover` 子命令**都不存在**。

**台股那条链已经通了、在用了**，这是 A 股要对齐的样板：

```text
stock_tw ──生产──→ ~/data/market/market_v2.duckdb ──消费──→ factor_research
 (台股 raw)         (2.3G, 2806支, 1992→, schema v5)      (因子研究, 实测 10M+ 行)
```

### 为什么这是个真实问题

- **研究被数据卡住**：`factor-research` 的基础设施（中性化、IC/报告链、因子纯函数）已跑通，但它只有台股数据。A 股因子研究现在**无从下手**。
- **不对齐 schema 就是重复造轮子**：`market_v2` 的表结构已被 `factor_research` 实测验证（10M+ 行加载）。A 股若另起一套 schema，研究代码就要写两遍，跨市场对比也无从谈起。
- **"有行数"不等于"可研究"**：幸存者偏差、下市历史缺失、单位不一致——这些都必须在入库前拦掉，否则污染的是下游全部研究结论。

### 为什么现在要做

- **数据源刚恢复**：2026-09-23 修复了 `free-stockdb`（换用 GitHub v0.3.5 发行版，原二进制有联网校验拒绝启动）。它支持 **日/周/月/1/5/15/30分钟/tick**——**日内能力是 Tushare 给不了的**，这正是用户后续要做日内的前提。
- **消费端已存在**：不像 History 协议层那样在等一个不存在的消费者，`factor_research` 现在就在读 `market_v2.duckdb`。
- **实现成本可控**：目标 schema 已冻结、来源接口已摸清、质量门禁代码已有，缺的是适配与落库那一段。

## 目标

### 本次要解决什么

**把 A 股日线从 `free-stockdb` 拉进一个与 `market_v2` 同构的 DuckDB，让研究代码零改动跨市场复用。**

四条具体目标：

1. **统一契约**：把 `market_v2` 的表结构固化成显式契约（列名/类型/主键/单位），作为 A 股的写入目标。
2. **来源适配**：`free-stockdb` 适配器——经 SSH 拉取、解 MessagePack、映射到统一契约。
3. **落库**：写入 `~/data/market/market_a.duckdb`，**表结构与 `market_v2` 逐列一致**，幂等可重跑。
4. **质量门禁**：复用 `check_candles()`，不合格的数据**不落库**。

### 不解决什么

- **不动 `market_v2.duckdb`**：它是 `stock_tw` 的产物，A 股数据写独立文件。
- **不做跨市场单库**：`market_v2.candles` 没有 `market` 列，而台股存在 6 位代码（137 个），与 A 股 6 位码会撞。合并需要改 `stock_tw` 的 schema，**明确后置**。
- **不做 History 协议层**：`market_protocol` 的 `HistoryRequest/Response` 服务于跨进程边界；当前生产者与消费者同机、消费者只有一个，加一层 protobuf 服务只增加故障面。协议已冻结，等第二个消费者出现再做。
- **不做实时/盘中**：本次只做历史拉取与落库，不接实时流。
- **不做分钟线**（M5 后置）：分钟数据量远大于日线，且日内的存储/查询设计需要单独验证；先把日线这条链路跑通。
- **不做 leaf→termux 复制**：原 PLAN 的快照同步语义不适用于 DuckDB 单文件写入，且当前无消费者。
- **不碰 `stock_tw`**：它是独立 repo、独立发布节奏。

## 约束

### 相容性要求

- **`market_a.duckdb` 的表结构必须与 `market_v2` 逐列一致**（列名、类型、语义）。这是"研究代码零改动复用"的前提，也是本项目的核心价值。契约以 `market_v2` 的实际 schema 为准（下方「表结构契约」）。
- **`factor_research` 的加载路径不能改**：它读 `candles` 表的 `symbol`/`date`/OHLCV，A 股库必须能被同一个加载器直接打开。
- **不修改 `market_v2.duckdb` 与 `stock_tw`**：一个字节都不动。
- **不修改 `market_protocol` 的 `.proto`**：本阶段不新增协议类型。
- 保留 `market_data = "market_data.cli:main"` 入口与现有 `manifest` / `verify-manifest` 子命令语义。

### 表结构契约（对齐目标，取自 `market_v2` schema v5）

```text
candles            symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
                   close DOUBLE, volume BIGINT, amount DOUBLE, turnover DOUBLE
adj_factor         symbol VARCHAR, date DATE, factor DOUBLE
shares_outstanding symbol VARCHAR, date DATE, shares BIGINT
symbols            symbol VARCHAR, name VARCHAR, market VARCHAR, status VARCHAR
```

- `symbol` 一律**字符串**（保留前导零，`000001` 不能变成 `1`）。
- `date` 是 `DATE` 类型，不是字符串、不是 epoch。
- `volume` 是 `BIGINT`（股），`amount` 是 `DOUBLE`（元）——单位必须与台股一致，否则跨市场对比是错的。
- 本阶段只落 `candles` / `adj_factor` / `shares_outstanding` / `symbols` 四张表；`fx_net_buy` / `margin` / `monthly_revenue` 是台股特有数据集，**A 股对应数据不存在或需另找来源，本次不落**。

### 性能要求

- 全市场日线（约 5600 支 × 多年）的首次全量拉取必须在**可接受时间内**完成（目标：单次运行 < 2 小时），且**支持中断续跑**——SSH 逐股拉取必然会被打断。
- 增量更新（每日新增一日）应在**分钟级**完成。
- 落库使用 DuckDB 批量写入（`executemany` / `INSERT ... SELECT`），不得逐行提交。

### 可维护性要求

- Python `>=3.12,<3.14`；本机 venv 3.12.12。
- CI 覆盖 `ruff format --check` + `ruff check` + `pyright` + `pytest`。
- 类型注解严格（`pyright` strict 已配置），错误用显式异常而非静默返回。
- 来源适配器与落库解耦：适配器只产出 DataFrame，不直接写库。

### 明确不能 break userspace 的点

- **`factor_research` 的读取代码**：它的 `load_standard_dataset(db_path, ...)` 依赖 `candles` 表结构。改这个结构 = 破坏用户。
- **`market_v2.duckdb`**：`stock_tw` 的生产库，正在被消费。**只读，永不写**。
- **`stock_tw` 的 CLI 默认路径**：2026-09-23 已改为指向 `~/data/market/market_v2.duckdb`，不要再改回去。
- **现有 `manifest` / `verify-manifest` 子命令**及其参数语义。

## 里程碑

### Milestone 1: 统一契约与 free-stockdb 来源适配

#### 目标
把 `market_v2` 的表结构固化成代码里的显式契约，并把 `free-stockdb` 的 A 股日线接进来，产出符合契约的 DataFrame。

#### 交付物

- `src/market_data/markets/contracts.py`：表结构契约常量（列名、类型、单位、主键），从 `market_v2` schema v5 推导，**作为唯一的对齐依据**。
- `src/market_data/markets/base.py`：来源适配器 `Protocol`（`fetch` / `normalize`），适配器只产出 DataFrame。
- `src/market_data/markets/cn/free_stockdb.py`：free-stockdb 适配器
  - **经 SSH 拉取**（`leaf` 上的 `127.0.0.1:7899` 不对外，Mac 不在其网段——见 `DEPLOY.md`）
  - 解 MessagePack（响应是 `application/x-msgpack`，**不是 JSON**）
  - 键格式：`日k:<code>:<YYYYMMDD>`，支持通配 `2026*` / `6*` 与范围
  - 字段映射到契约（源有 21 字段，含 `turnover`/`pe_ttm`/`total_mv` 等；只取契约内的）
  - 批量拉取按**代码前缀**（`0`/`3`/`6`/`1`/`5`/`9`）分组，不得逐股一次请求
- `src/market_data/markets/cn/__init__.py`：稳定导出。
- `tests/fixtures/free_stockdb/`：真实 msgpack 响应样本（至少含一只沪市股 + 一只深市股）。
- `tests/test_contracts.py`、`tests/test_free_stockdb_adapter.py`：字段/类型/前导零/排序/空值/错误输入。

#### 验收标准

- 契约常量与 `market_v2` 的实际 `PRAGMA table_info` **逐列一致**（有测试断言，防止漂移）。
- 用 fixture 能产出列名/类型完全符合契约的 DataFrame。
- `000001` 等前导零代码在标准化前后**保持字符串原样**。
- 真实拉取一次 `600633` 的近 5 个交易日，字段值与 `free-stockdb` 原始响应一致。
- 适配器单测**不访问网络、不启动 SSH**（用 fixture）。

#### 风险

- **SSH 通道稳定性**：leaf 经 xiaoxin 跳板，链路比直连脆弱。需要超时、重试与**续跑**，否则 5600 支拉不完。
- **free-stockdb 的二进制会再次过期**：2026-09-23 就是旧构建（有联网校验）拒绝启动导致的。适配器需要能明确区分"服务不可达"与"数据没有"。
- **源字段语义不确定**：`volume` 的单位（股/手）、`amount` 的单位（元/万元）未验证。**必须先抽样核对再入库**——单位错会让全部跨市场对比失真。
- **msgpack 的键类型**：实测 `date` 是整数（`20260828`）而非字符串，解析时必须显式转换。

---

### Milestone 2: 落库到 DuckDB（market_v2 同构）

#### 目标
把 M1 产出的 DataFrame 写进 `~/data/market/market_a.duckdb`，表结构与 `market_v2` 一致，且**幂等可重跑**。

#### 交付物

- `src/market_data/offline/duckdb_store.py`：
  - 建表（`CREATE TABLE IF NOT EXISTS`，DDL 从契约生成，不手写）
  - 幂等写入：以 `(symbol, date)` 为主键 `INSERT ... ON CONFLICT DO UPDATE`，重复运行结果一致
  - `schema_version` 表，与 `market_v2` 同构，便于将来识别版本
- `tests/test_duckdb_store.py`：建表、幂等、冲突更新、类型保持。

#### 验收标准

- 写入后能被 **`factor_research` 的加载器直接打开**（用一个最小调用验证，不必跑完整研究流程）。
- 同一批数据连续写入两次，行数与内容不变。
- `symbol` 在库内仍是字符串且保留前导零；`date` 是 `DATE` 类型。
- 库中表结构与 `market_v2` 的 `PRAGMA table_info` 逐列一致（测试断言）。

#### 风险

- **schema 漂移**：`market_v2` 是 `stock_tw` 的产物，它升级 schema（现在是 v5）时我们不会自动知道。→ M1 的契约一致性测试是唯一防线，需在 CI 跑。
- **主键选择**：`market_v2` 的 `candles` 是否真有 `(symbol, date)` 唯一约束**尚未确认**。若无，我们的"幂等"只能靠写入端保证。
- **单文件并发**：DuckDB 对同一文件的多写者会加锁。若将来 `stock_tw` 与 `market_data` 都要写，需重新设计（当前 A/TW 分文件，暂无此问题）。

---

### Milestone 3: 质量门禁与失败不落库

#### 目标
**"有行数"不等于"可研究"。** 不合格的数据必须被拦下，且失败时库里保持原样。

#### 交付物

- 把现有 `offline/quality.py::check_candles()` / `require_clean()` 接进落库前的管线。
- `src/market_data/offline/pipeline.py`：`拉取 → 标准化 → 门禁 → 落库` 的单向流程；**任一步失败都不写库**。
- 加载溯源：复用 `offline/manifest.py` 记录"本次拉了哪个日期区间、多少行、校验值"，写入 `market_a` 库或旁路 JSON。
- `tests/test_pipeline.py`：坏数据被拒、失败不落库、成功后库内容正确。

#### 验收标准

- 构造非法 fixture（OHLC 越界、重复主键、非法日期、负值、NaN）→ **全部被拒且库无变化**。
- 门禁失败时返回明确错误，不是静默跳过。
- 溯源记录能回答"库里这批数据是哪次拉的"。

#### 风险

- **规则过严会拒掉合法数据**：A 股有停牌、ST、退市整理期等特殊情况（源里就有 `is_st` 字段）。**M3 已按真实样本校准**：每条规则的实测命中数记在 `offline/quality.py` 的模块注释里，证人 fixture 在 `tests/fixtures/free_stockdb/`（每个边界类别一个真实响应）。实测推翻了原计划的两条——`pct_chg` / `pre_close` 不可信（000010 有 44.1% 的行与 `close/pre_close-1` 矛盾），且**不建任何涨跌幅规则**（ST 股单日实测到 17.29% / 22.78%，是真实的停牌复牌跳空，按 `is_st` 分档同样拦不住）。
- **重复主键的处理语义** ✅ **已决**：**拒绝整个批次**，不做「取最后一条」。M2 的 `duckdb_store._require_unique_keys()` 与 M3 的 `check_candles()` 两层语义相同，只是报告粒度不同。依据是实测：DuckDB 1.5.5 在同一条 `INSERT` 内遇到重复键会**静默保留第一条**。原原则「不在标准化阶段静默去重」保持不变——`normalize()` 不去重。

---

### Milestone 4: 编排与 CLI

#### 目标
一条命令完成"拉取 → 标准化 → 门禁 → 落库"，支持增量与续跑，失败可诊断。

#### 交付物

- CLI 子命令（扩展 `cli.py`，保留现有 `manifest` / `verify-manifest`）：
  - `update --market cn --start <date> --end <date>`：增量更新
  - `update --market cn --full`：全量重建
  - 进度与结构化日志；断点续跑（记录已完成的代码前缀/日期）
- 告警：复用 `offline/alerts.py`，失败时发飞书（依赖 `lark-alert` 可选 extra）。
- 失败路径有明确退出码。
- `tests/test_cli.py`。

#### 验收标准

- 全量拉取能在目标时间内完成，中途 Ctrl-C 后**重跑能从断点继续**。
- 增量更新（新增一个交易日）在分钟级完成。
- 服务不可达（leaf 关机 / stockdb 未启动）时给出**明确错误**，不是空结果。
- 告警失败不把流程改判为成功，也不隐藏原始失败原因。

#### 风险

- **5600 支 × SSH 逐批**的耗时可能远超预期 → 需要先测单批耗时再估总量；必要时调整批量粒度。
- **断点状态的正确性**：续跑状态写错会导致静默漏数据——这比慢更危险，需要测试覆盖。
- **leaf 的稳定性**：那是 2013 年的老机器，且 `stockdb` 需要常驻。它的可用性直接决定本管线能否按日推进。

---

### Milestone 5: 分钟线（日内，后置）

#### 目标
把 `free-stockdb` 的 1/5/15/30 分钟线纳入，为后续日内研究铺路。**待日线链路稳定后再启动。**

#### 交付物

- 分钟表的 schema（需先确认：是复用一张表加 `period` 列，还是独立表——`market_v2` **没有分钟表可对齐**，这是新的设计）。
- 分钟线适配器与落库。
- 存储与查询性能验证。

#### 验收标准

- 分钟数据可被研究代码消费，且能回答"某标的某日的分钟序列"。
- 存储占用与查询延迟在可接受范围。

#### 风险

- **无对齐样板**：`market_v2` 只有日线，分钟表的 schema 要自己定；定错了将来改成本高。
- **数据量**：23G 里分钟占大头。全市场全历史分钟线可能远超预期，需要先量再定范围（例如只保留近 N 年）。
- **单位与时段语义**：集合竞价、午休、停牌的处理需要与源逐一对齐。

## 兼容性检查

**必须保持不变的行为**

- `market_v2.duckdb`：只读。任何情况下不得写入、不得改表结构。
- `factor_research` 的加载器：它读 `candles` 的 `symbol`/`date`/OHLCV。`market_a.duckdb` 必须能被同一个加载器打开。
- `stock_tw` 的 `DEFAULT_DB_PATH`：指向 `~/data/market/market_v2.duckdb`，不要改回相对路径。
- `market_data` 包入口 `market_data = "market_data.cli:main"` 与现有 `manifest` / `verify-manifest` 子命令的参数语义。
- 现有 `offline/` 的公开函数签名（`build_manifest` / `verify_manifest` / `check_candles` / `require_clean` / `publish_snapshot`）。

**不能被破坏的接口**

- `candles` 表结构（对齐 `market_v2`）。
- `market_a.duckdb` 的路径（`~/data/market/market_a.duckdb`）——消费者会依赖它。

**如果必须调整，迁移路径是什么**

- **`market_v2` 升级 schema**：M1 的契约一致性测试会红。届时**先确认 `stock_tw` 的新语义**，再决定 A 股库是否跟进；不跟进的代价是跨市场对比失效，必须显式记录。
- **`free-stockdb` 字段变化**：适配器的字段映射层是唯一的改动点；增加字段不必改契约（只取契约内的），删除字段会导致门禁失败也就是显式失败。
- **已有代码的处置**：`offline/publish.py`（快照 symlink 切换）在 DuckDB 单文件写入的模型下**没有消费者**——它服务的是"快照目录 + current 软链"语义，而本 PLAN 落库不产生快照目录。**保留但标注为非主路径**；若确认长期不用应删除，不留死代码。

## 退出条件

**什么算完成**

- `market_a.duckdb` 里有**可研究的 A 股日线历史**：覆盖足够长的区间、无缺口、单位正确。
- `factor_research` 不改一行代码就能加载 A 股数据并跑通完整研究流程。
- 连续 5 个交易日按日增量更新，无缺口、无重复、无静默失败。
- 这条链路的存在，使"做 A 股因子研究"不再被数据卡住——这是唯一的成功标准。

**什么情况下应停止继续扩展**

- **单位/语义存疑未澄清**：`volume`/`amount` 的单位若无法与源确认，**停**。宁可不做，不可做错——错误单位会污染全部下游研究结论。
- **数据源不稳定**：`free-stockdb` 若再次出现"服务不可用"（二进制过期、授权变化），先修源，不在坏源上加功能。
- **跨市场单库的需求涌上来**：那需要改 `stock_tw` 的 schema（加 `market` 列），是独立决策，不塞进本 PLAN。
- **分钟线需求早于日线稳定**：日线这条链路（M1–M4）未跑通前不开 M5。
- **判断标准**：这是不是真问题？有更简单的办法吗？会破坏什么吗？
