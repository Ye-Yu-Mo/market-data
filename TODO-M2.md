# TODO-M2: 落库到 DuckDB（market_v2 同构）

---

## 目标

- **本任务要解决的问题**：M1 已经能产出符合契约的 DataFrame，但数据还在内存里。A 股要能被研究，必须落到一个与 `market_v2` 逐列同构的 DuckDB 文件（`~/data/market/market_a.duckdb`），消费者才能零改动读取。本任务实现建表与幂等写入。
- **所属 Milestone**：M2。M1 已交付契约与适配器；M3 做质量门禁；M4 做编排与 CLI。
- **本轮边界**：只做「写」。不做质量门禁（M3）、不做 CLI/调度（M4）、不做增量编排。

> **一条必须先读的实测结论**：`market_v2.adj_factor` 是**日频**表（10,700,422 行，与 `candles` 逐行一一对应；2330 的 factor 日期是连续的交易日 09-13/14/15/16/17/**21**/22/23），**不是除权事件表**。而 M1 的 `normalize_factors()` 返回的是**事件序列**（600633 只有 25 行）。直接写入会让下游把 5424 行砍成 25 行——**而 `PLAN.md` 指定的验收方式抓不到它**。这是本 Milestone 最重要的一件事，见功能3。

---

## 功能1: 建库与建表

### 修改1: 新增 `A_SHARE_DB_PATH` 常量，并加一道「绝不写 market_v2」的守卫

- **改什么**：在 `markets/contracts.py` 增加 `A_SHARE_DB_PATH = Path.home() / "data" / "market" / "market_a.duckdb"`（与既有的 `REFERENCE_DB_PATH` 并列）；写入端在真正建连接**之前**断言目标不等于 `REFERENCE_DB_PATH`，相等就抛异常。
- **为什么改**：`PLAN.md:99` 的铁律是「`market_v2` 只读，永不写」。当前只靠纪律，没有代码保证——一次手滑的路径拼接就能毁掉台股 2.3G 生产库。守卫要挡在建连接之前，而不是写完再检查。
- **预期结果**：把 `A_SHARE_DB_PATH` 指向 `market_v2.duckdb` 时立即失败，且**在文件被打开之前**失败。

### 修改2: 建表复用 `contracts.create_table_sql()`，不手写 DDL

- **改什么**：`open_store(path)` 打开文件后，对 `MARKET_V2_TABLES` 逐张执行 `CREATE TABLE IF NOT EXISTS <create_table_sql(contract)>`。
- **为什么改**：`PLAN.md:150` 要求「DDL 从契约生成，不手写」。`create_table_sql()` M1 已交付，且**无条件给所有标识符加引号**——`close` 这个保留字因此不需要任何特判。再手写一遍 DDL 就多了一份会和契约漂移的副本。
- **预期结果**：建出来的表用 `read_contract()` 读回来与契约常量**完全相等**（`TableContract` 是 dataclass，直接 `==` 比对，与 `tests/test_contracts.py` 对 `market_v2` 的比对同构）。

### 影响文件

- `src/market_data/markets/contracts.py`（新增路径常量）
- `src/market_data/offline/duckdb_store.py`（新增）
- `src/market_data/offline/__init__.py`（导出）

---

## 功能2: 幂等批量写入

### 修改1: `candles` 用 `INSERT ... SELECT` + `ON CONFLICT DO UPDATE` 批量落库

- **改什么**：把 DataFrame 注册成 DuckDB 视图，一条 `INSERT INTO candles SELECT ... FROM df ON CONFLICT (symbol, date) DO UPDATE SET open=excluded.open, ...` 写完一批，全程不逐行。
- **为什么改**：`PLAN.md:87` 明令「不得逐行提交」。且实测已确认 `candles` **确有 `PRIMARY KEY(symbol, date)`**（`PLAN.md:164` 把这条列为「尚未确认」的风险，可以关掉）——`ON CONFLICT` 的冲突目标直接可用，不需要额外建唯一索引。
- **预期结果**：同一批数据连写两次，`count(*)` 与内容都不变；改动其中一行的值再写，该行被更新而非产生重复。

### 修改2: 类型保真——`date` 落成 DATE、`symbol` 保持字符串、`volume` 落成 BIGINT

- **改什么**：写入前显式把 `date` 从 `datetime64[s]` 转成 `date`；`symbol` 保持 `object`；`volume` 保持整数。
- **为什么改**：`PLAN.md:158` 的验收要求「`symbol` 在库内仍是字符串且保留前导零；`date` 是 `DATE` 类型」。M1 的 `normalize()` 出口 dtype 已是 `object`/`datetime64[s]`/`int64`，但**跨 DuckDB 边界必须有断言**——`000001` 一旦在某一层被当成数字就再也回不来。
- **预期结果**：写完后 `SELECT symbol FROM candles WHERE symbol='000001'` 能命中；`PRAGMA table_info` 显示 `date` 是 `DATE`、`volume` 是 `BIGINT`；用 `typeof()` 抽查确认。

### 影响文件

- `src/market_data/offline/duckdb_store.py`
- `tests/test_duckdb_store.py`（新增）

---

## 功能3: `adj_factor` 展开成日频序列（**本 Milestone 最关键**）

### 修改1: 把除权事件序列展开成与 `candles` 逐日对齐的序列

- **改什么**：新增纯函数（建议放 `markets/cn/free_stockdb.py`，与 `normalize_factors` 并列），签名形如 `daily_factors(events: pd.DataFrame, trading_days: pd.DataFrame) -> pd.DataFrame`：对每一个 `(symbol, date)` 的交易日，取**该日或之前最近一次除权事件的 `cum`**；该日之前没有任何事件时按源 SDK 的口径取 `cum = 1.0`；最后 `factor = cum / cum_latest`。
- **为什么改**：**这是实测出来的、`PLAN.md` 完全没写的一条。**
  - `market_v2` 里 `candles` 与 `adj_factor` **行数完全相同**（各 10,700,422），且**双向零缺口**（candles 无 factor 的行 = 0，factor 无 candles 的行 = 0）。
  - 2330 的 factor 日期是连续交易日（`1994-09-13 … 09-17, 09-21 …`，跳过的 18/19 是周末），不是事件日。
  - 而 M1 的 `normalize_factors()` 返回**事件序列**：600633 全历史只有 **25 行**。
  - **后果**：下游 `factor_research` 的加载 SQL 是 `FROM candles c JOIN adj_factor a USING (symbol, date)`（`qlib/duckdb.py:29-37`，**INNER JOIN**）。只写 25 行事件，600633 的 5424 行会被**静默砍成 25 行**——不报错，只是历史凭空少 99.5%。
  - **而 `PLAN.md:156` 指定的验收（「用一个最小调用验证能被加载器打开」）会照常通过**——它打开成功、返回 25 行、不抛异常。这个验收标准必须补强（见验收标准）。
- **预期结果**：`adj_factor` 的行数、`(symbol, date)` 集合与 `candles` **完全相等**；有测试断言这个相等关系，而不是只断言「能打开」。

### 修改2: `cum_latest` 会平移，所以写入只能覆盖、不能只追加

- **改什么**：`adj_factor` 的写入同样走 `ON CONFLICT (symbol, date) DO UPDATE SET factor = excluded.factor`。
- **为什么改**：M1 的 `normalize_factors` 文档已写明：`cum_latest` 随每次新除权变化，**整条历史序列会平移**，不是追加一行。若写入端用 `DO NOTHING` 或只追加新日期，除权之后旧日期的 factor 会全部停留在旧基准上——错误是静默的，且会污染全部前复权价。
- **预期结果**：构造一批数据，先写入，再改变最后一个事件的 `cum` 重写，断言**所有历史行的 factor 都变了**（证明覆盖生效）。

### 影响文件

- `src/market_data/markets/cn/free_stockdb.py`（新增展开函数）
- `src/market_data/markets/cn/__init__.py`（导出）
- `src/market_data/offline/duckdb_store.py`
- `tests/test_duckdb_store.py`、`tests/test_free_stockdb_adapter.py`

---

## 功能4: `symbols` 与 `schema_version`

### 修改1: `symbols` 落库（与 `market_v2` 的差异是刻意的）

- **改什么**：把 M1 `build_symbols()` 的产物写入 `symbols` 表。
- **为什么改**：`market_v2` 的 `symbols` 表**实测是空表（0 行）**。我们写入 **5837 行**（5510 在市 + 339 个去重退市代码 − 12 个交集）是**刻意的差异**：`PLAN.md:27` 明确把「下市历史缺失、幸存者偏差」列为要在入库前拦掉的问题，而 `退市*` 那 339 个不同代码就是解药（注意：该接口返回 918 *条*，同一代码最多重复 47 次，必须折叠）。表结构同构，内容更多，不破坏任何消费者。
- **预期结果**：`symbols` 的行数 = 代码表股票组 + 退市组；`status` 只出现 `listed` / `delisted`（`CHECK` 约束会替我们挡住第三个值）；**在注释里写明这是与参考库的刻意差异**，避免后人当成 bug 去「修」。

### 修改2: `schema_version` 的写入语义要显式决定并记录

- **改什么**：往 `schema_version` 写入我们实际建立的版本。
- **为什么改**：实测 `market_v2.schema_version` 是 **1..5 五行**，语义是**迁移历史集合**（`stock_tw` 每个迁移插一行、当前版本取 `MAX(version)`），**不是单一版本号**。`markets/contracts.py` 的文档已写明「写单行还是五行是一个要刻意做的决定」。照抄 `MAX(version)=5` 一行，与照抄五行，语义不同。
- **预期结果**：**建议写 `5` 单行**（我们不回放 1–4 的迁移，直接以 v5 形状建库），并在 `duckdb_store.py` 的注释里写明这个选择及其理由。无论选哪个，都要有测试固定住，避免下次有人「顺手改成一样」。

### 影响文件

- `src/market_data/offline/duckdb_store.py`
- `tests/test_duckdb_store.py`

---

## 功能5: `shares_outstanding` —— 需要显式决定「本 Milestone 落不落」

### 修改1: 决定并记录

- **改什么**：二选一并写进注释——
  - **(a) 本 Milestone 不落**：契约里有这张表，建表照建，但不写数据。
  - **(b) 落**：新增一个纯函数把源行的 `total_share` / `float_share` 映射到 `shares(date)`。
- **为什么改**：契约含 `shares_outstanding`，但 **M1 没有任何适配器函数产出它**（`build_symbols` 产 symbols，`normalize` 产 candles，`normalize_factors` 产 adj_factor）。源里每行日线都带 `total_share` 和 `float_share`，所以有数据可用——但**映射到哪一列是个语义决定**：下游 `factor_research` 用 `shares` 算 `volume/shares`（换手）和 `close*shares`（市值），而 `market_v2` 的 `shares_outstanding` 来自台股的「股本」。选 `total_share` 还是 `float_share`，会得到不同的换手率定义。
- **预期结果**：无论选哪个，都要在契约注释里写明「`shares` 取的是源的总股本还是流通股本」，并说明它对下游因子的影响。**若选 (a)，必须在注释里说明缺少它会让哪些因子跑不了**（`research/turnover.py`、`research/mktcap.py`），因为下游是 `LEFT JOIN`，缺失不报错。

### 影响文件

- `src/market_data/offline/duckdb_store.py`
- `src/market_data/markets/contracts.py`（注释）
- 若选 (b)：`src/market_data/markets/cn/free_stockdb.py`

---

## 功能6: 消费者兼容性验证

### 修改1: 用 `factor_research` 的真实加载 SQL 验证，而不是「能打开」

- **改什么**：在测试里用 `FACTOR_RESEARCH_DUCKDB`（或直接传 `--db`）指向临时库，调用 `load_standard_dataset()`，断言**行数与源一致**，而不只是「没有抛异常」。
- **为什么改**：`PLAN.md:156` 的验收写的是「能被加载器直接打开」。实测表明这句话**不足以发现功能3 那个 99.5% 的静默丢行**——库能打开、SQL 能跑、返回 25 行，一切「正常」。必须断言行数。
- **预期结果**：从 `candles` 到 `load_standard_dataset` 的返回，行数等于满足 `close > 0 AND volume > 0` 的 candles 行数；若不等，测试失败并打印差额。
- **注意（写测试时会撞上）**：`factor_research` 的加载器有两条硬约束，测试夹具必须满足——
  1. `adj_factor` 是 **INNER JOIN**，所以夹具必须给每根 K 线都配 factor（这正是功能3 在做的事）。
  2. SQL 里有 `WHERE c.close > 0 AND c.volume > 0`，**A 股停牌（volume = 0）的整行会被消费端过滤**。这是消费端行为，不是我们的 bug；测试断言要按这个口径算期望行数（`PLAN.md` 未提及此过滤）。
  3. 加载器**不使用 `symbols` 表**，市场归属靠调用方传的 `market_resolver`；现成脚本的启发式是 `len(symbol) == 4 → TWSE else TPEX`，**A 股 6 位码会全被判成 TPEX**。验证时要显式传 resolver，且**不要**顺手去改 `factor_research` 的代码。
- **依赖方向**：这是**测试期**的跨仓库引用。若 `factor_research` 不在预期路径，测试应 `skip` 并给出原因，而不是失败或伪装通过。

### 影响文件

- `tests/test_duckdb_store.py`
- `tests/test_consumer_compat.py`（新增，若拆开更清晰）

---

## 测试计划

- **单元测试**：
  - 建表：DDL 建出的表用 `read_contract()` 读回，与契约常量**相等**（5 张表逐张）。
  - 幂等：同一批写两次，行数与内容不变。
  - 冲突更新：改一行的值重写，该行被更新、行数不变。
  - 类型保持：`symbol` 是 `VARCHAR` 且 `000001` 原样；`date` 是 `DATE`；`volume` 是 `BIGINT`。
  - 路径守卫：把目标指向 `market_v2.duckdb` 时，抛异常且**文件未被打开**。
  - `daily_factors()`：事件展开成日频的正确性——区间内取最近事件、事件当日生效、首个事件之前取 `cum = 1.0`、末值恒为 1.0。
  - 覆盖写：改变 `cum_latest` 后重写，历史行全部更新（证明不是只追加）。
- **集成测试**：
  - 用 M1 抓的真实 fixture（`tests/fixtures/free_stockdb/`）走完整链路：`normalize` → 写 candles → `normalize_factors` + `daily_factors` → 写 adj_factor → 断言两表 `(symbol, date)` 集合相等。
  - 用真实 fixture 的 `codes.bin` / `delisted.bin` 走 `build_symbols` → 写 symbols → 断言行数与 `status` 取值域。
- **回归测试**：
  - M1 的 114 个测试**零改动**通过。
  - `ruff format --check` + `ruff check` + `pyright` + `pytest` 全绿。
  - `market_v2.duckdb` 的 sha256 在测试前后不变（一条廉价的「真的没写它」证据）。
- **边界条件测试**：
  - **空 DataFrame**：写入空批次不报错、不改变库（与 M1 的 `SourceEmpty` 语义保持一致，不静默当成功）。
  - **重复主键**：同一批里出现重复 `(symbol, date)` 时的行为要显式——`ON CONFLICT` 在**同一条 INSERT 内部**的重复行上会报错还是覆盖，必须先测清楚再定语义；`PLAN.md:190` 的原则是「不在标准化阶段静默去重」。
  - **除权事件早于首根 K 线 / 晚于末根 K 线**：展开函数要给出确定行为，不能抛 `IndexError` 或产出 NaN。
  - **`adj_factor` 有事件但 `candles` 没有任何行**（退市股只拉到复权的情况）：断言不会写入孤立的 factor 行（参考库实测这种行数为 0）。
  - **并发/重复打开**：同一文件被第二个写者打开时的行为要有明确测试或明确文档（DuckDB 单写者会加锁）。

---

## 验收标准

- [ ] `market_a.duckdb` 建出后，5 张表的结构用 `read_contract()` 读回与契约常量**逐列相等**（有断言）。
- [ ] 表结构与 `market_v2` 的 `PRAGMA table_info` + `duckdb_constraints()` 逐列一致（沿用 M1 `tests/test_contracts.py` 的比对方式，对 `market_a` 再跑一遍）。
- [ ] **`adj_factor` 与 `candles` 的 `(symbol, date)` 集合完全相等**（双向零缺口，有断言）。**这一条是 M2 的核心，不可省略。**
- [ ] 同一批数据连续写入两次，行数与内容不变。
- [ ] `symbol` 在库内是字符串且 `000001` 保留前导零；`date` 是 `DATE`；`volume` 是 `BIGINT`。
- [ ] `factor_research` 的 `load_standard_dataset()` 能打开该库，且**返回行数等于**满足其过滤条件的 `candles` 行数。
- [ ] 把目标路径指向 `market_v2.duckdb` 时被守卫拒绝，且文件哈希不变。
- [ ] `symbols` 覆盖代码表股票组 + 退市组，`status` 只有 `listed` / `delisted`。
- [ ] `schema_version` 的写入语义有注释说明 + 测试固定。
- [ ] `shares_outstanding` 落或不落**有明确记录**，落的话写明取的是总股本还是流通股本。
- [ ] `ruff format --check` + `ruff check` + `pyright` + `pytest` 全绿。

---

## 兼容性检查

- **是否影响现有行为**：
  - M1 的 `markets/` 与 `offline/` 四件套（`manifest` / `quality` / `publish` / `alerts`）**公开签名不变**；本 Milestone 只新增 `offline/duckdb_store.py` 与一个展开函数。
  - CLI 现有 `manifest` / `verify-manifest` 语义不变（M2 不新增子命令）。
  - **`market_v2.duckdb` 与 `stock_tw` 一个字节都不动**：所有对参考库的连接一律 `read_only=True`，且有哈希断言。

- **是否需要兼容旧接口/旧数据**：
  - 本轮全是新增（新文件、新表），无旧接口。
  - 但**写入必须是可重跑的**：M2 之后 M3/M4 会对同一个库反复写同一批数据，`ON CONFLICT DO UPDATE` 是这条的前提。若某张表用了 `INSERT`（无冲突处理），重跑会直接报主键冲突——这要在 M2 就测出来，而不是等 M4。

- **是否存在 break userspace 风险**：
  - **有，且都集中在「静默丢行」这一类**，因为消费者用 INNER JOIN 且不报错：
    1. **`adj_factor` 不全 = 历史被静默截断**（功能3）。这是本轮最大的风险，且 `PLAN.md` 原定的验收方式**测不出来**。
    2. **`amount` 写 NULL** 会被消费端填成 `Decimal(0)`，不报错但污染换手/资金流因子。M1 已保证 A 股源必有 amount，M2 的测试要断言写出后 `amount` 无 NULL。
    3. **`open` 写 0 而不是 NULL** 会改变 bar 的语义（消费端对 `open IS NULL` 有专门处理，对 0 没有）。M1 已按 NULL 处理，M2 要断言这个 NULL 被原样落库。
    4. **`turnover` 全 NULL 是刻意的**：`market_v2` 该列是**成交笔数**，A 股源给的是**换手率百分数**，两者不是同一个量，M1 已决定丢弃。M2 落库时保持 NULL，并在注释里写明原因——否则后人看到一列全空会去「修」它。
  - **`shares_outstanding` 的语义选择会影响下游因子定义**：`volume/shares`（换手）与 `close*shares`（市值）都依赖它，选总股本还是流通股本是**研究结论层面的差异**，不是实现细节（功能5）。
  - **参考库升级 schema 时**：M1 的契约一致性测试会先红，这是设计好的防线；但**该测试目前没有 CI 可跑**（仓库无 `.github/workflows/`、无 remote），而 `PLAN.md:163` 称它是「唯一防线」。M2 结束时要么补 CI，要么把这句话从 PLAN 里删掉——**不要留着一条不存在的防线**。
