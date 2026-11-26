# TODO-M1: 统一契约与 free-stockdb 来源适配

> 本文只含 M1 的**开发**工作。部署、运行时、调度、运维**不在本文范围**。
> 所有源侧结论基于 2026-09-23 对 leaf `127.0.0.1:7899` 的只读实测，标「实测」的都有原始字节支撑。
> 与 `PLAN.md` / `DEPLOY.md` 冲突处**以本文为准**，冲突清单见第二节。

---

## 目标

把 `market_v2` 的表结构固化成代码里的显式契约，并把 `free-stockdb` 的 A 股日线接进来，产出符合契约的 DataFrame。

**本轮边界**：只做到 DataFrame。不写 DuckDB（M2）、不接质量门禁（M3）、不做 CLI（M4）、不做调度。

---

## 一、已验证的源契约（写代码的依据）

### 1.1 响应格式：MessagePack

```
Content-Type: application/x-msgpack
```

单条 `日k:600633:20260625` 的原始字节 `de 00 15` = **map16 + 21 个键**。21 字段全解码，与 vendor 文档样本（`调用方式/http/rd_test.py:35`）**逐字段一致**。

- `code` 是 **str(6)**（源本身保留前导零）、`date` 是 **uint32** 整数
- 字段顺序实测固定：`date, code, name, open, high, low, close, pre_close, volume, amount, turnover, pct_chg, amplitude, is_st, vol_ratio, total_share, float_share, total_mv, float_mv, pe_ttm, pb`

**`pyproject.toml` 必须加 `msgpack` 依赖**（当前没有）。解码用 `msgpack.unpackb(raw, raw=False, strict_map_key=False)`——`raw=False` 否则 `code` 会变成 `b'600633'`。

### 1.2 查询语法（实测）

```
GET /?cmd=<cmd>&t=<table>&k1=<k1>&k2=<k2>[&num=N][&ap=<投影>]
```

| 参数 | 实测可用值 |
|---|---|
| `cmd` | `get` / `vals` / `keys` |
| `t` | `日k` / `分钟k` / `复权` / `股票代码` |
| `k1` | `key:<code>` / **`qz:<前缀>`** / `all:` |
| `k2` | `key:<YYYYMMDD>` / `fwd:<起>,<止>` / `all:` |
| `num` | 服务端切片：`N` 取前 N、`-N` 取末 N |

**批量路径实测**：

| 查询 | 体积 | 含义 |
|---|---|---|
| `k1=all:&k2=key:20260831` | **2,373,655 B** | 全市场某一日，**一次请求** |
| `k1=qz:6006&k2=key:20260831` | 24,997 B | 一个前缀的某一日 |
| `k1=qz:6006&k2=all:` | **179,095,566 B** | 一个前缀全历史（179MB） |
| `k1=key:6006*&k2=key:20260831` | **1 B（空）** | **通配在 `k1` 上不生效** |

⚠️ **`PLAN.md:119` 的「按代码前缀分组」写法是错的**：必须 `k1=qz:<前缀>`，不是 `key:<前缀>*`。
⚠️ **内存**：单响应可达 179MB。不得整包缓冲前缀全历史，必须分块。

### 1.3 返回结构（四种，必须分开处理）

| `cmd` | 形态 |
|---|---|
| `vals` | 数组套 map：`[{21字段}, ...]`，**无键** |
| `get`（单键） | 裸 map |
| `get`（多键） | 数组套二元组：`[["日k:600633:20260625", {...}], ...]` |
| `keys` | 字符串数组：`["复权:600633:19930705", ...]` |

**`cmd=vals` 会丢掉日期键**——需要日期的场景（复权）必须用 `get` / `keys`。

### 1.4 字段与单位（**在真实字节上验证通过**）

5 个真实交易日逐行验算：

| 断言 | 结果 |
|---|---|
| `amount / volume ∈ [low, high]` | **5/5 通过**（20260625: 10.4822 ∈ [10.37, 10.62]） |
| `volume / float_share × 100 == turnover` | **5/5 通过**（20260625: 1.42196 vs 1.42） |

→ **`volume` 单位股、`amount` 单位元、`turnover` 是百分数**（`1.42` = 1.42%），价格**不复权**。

**这两个断言必须写成代码**——结论是从样本反推的，不是文档承诺（全仓库 grep「单位」无一处是日k）。`PLAN.md:278` 的停止条件就是「单位存疑 → 停」。

### 1.5 复权因子

```python
["复权:600633:19930705", {"div": 0.0,  "give": 0.5,  "trans": 0.0,   "mult": 1.1,   "cum": 1.1}]
["复权:600633:20260612", {"div": 0.17, "give": 0.0,  "trans": 0.0,   "mult": 1.016, "cum": 5.649}]
```

`cum` **单调递增**，是**后复权**基准。而 `market_v2.adj_factor.factor` 实测是**前复权**（值 ≤ 1，最新交易日恒为 1.0；全库 1,618,114 行 == 1.0）。

**正确映射：`factor = cum / cum_latest`。**

### 1.6 三个会静默出错的源行为

1. **不存在的 code 返回 HTTP 200 + `0x90`（空数组）**，不是 4xx/5xx。参数写错也长这样。
2. **顺序不稳定**：`fwd:20260622,20260626` 返回**降序**；`all:` + `num=1/-1` 是升序。→ **必须显式排序**。
3. **数值类型不稳定**：同一个 `amount`，20260626 是 int，20260622 是 float（`258471424.0`）。`volume` 同理。→ **必须显式转型**。

### 1.7 代码表与退市

| 项 | 实测值 |
|---|---|
| `cmd=get&t=股票代码` | map，6 组：`0→1493`、`1→995`、`3→1397`、`5→1058`、`6→2311`、`9→309`，**合计 7563** |
| 其中 A 股股票（0/3/6/9） | **5510**；`1`/`5` 是基金/债 |
| `cmd=vals&t=退市*` | **918 条 / 339 个不同代码**（扁平字符串数组；源按表名前缀匹配，同一代码最多重复 47 次，`build_symbols` 会按 symbol 折叠） |
| 600633 覆盖 | 20000107 → 20260831（从 IPO 起） |

---

## 二、与 `PLAN.md` 不同、以本文为准的点

| 位置 | 原文 | 更正 |
|---|---|---|
| `PLAN.md:115` | 「适配器**经 SSH 拉取**」 | **删掉**。适配器只接受 `base_url`，取数走普通 HTTP；SSH 是传输层的事，不属于适配器（见功能2） |
| `PLAN.md:119` | 「按代码前缀分组」 | 语法是 **`k1=qz:<前缀>`**，不是 `key:<前缀>*` |
| `PLAN.md:68-76` | 契约表（手抄） | **漏了可空性与 CHECK**：`open`/`amount`/`turnover` 可空；`symbols` 有 `CHECK (status IN ('listed','delisted','suspended'))` |
| `PLAN.md:164` | 「`(symbol,date)` 唯一约束尚未确认」 | **已确认有 `PRIMARY KEY(symbol, date)`** |
| `PLAN.md:121` | 「真实 msgpack 响应样本」 | **正确，保留** |
| `PLAN.md:124` | 「不得逐股一次请求」 | 仍然成立，但正确粒度是**一日一次**（`k1=all:`），不是按前缀 |
| `PLAN.md:134` | 「SSH 通道稳定性 → 需要超时、重试与续跑」 | 适配器不碰 SSH。HTTP 的超时/重试仍要做，但**续跑是 M4 的事，不是 M1** |
| `PLAN.md:136` | 「单位不确定 → 必须先抽样核对」 | **已核对通过**（1.4），固化成断言即可 |
| `PLAN.md:31` | free-stockdb「支持日/周/月」 | 实测只有 `日k` / `分钟k`；**周/月是客户端聚合** |

---

## 三、交付物

### 功能1: `src/market_data/markets/contracts.py`

#### 1.1 契约常量从 `PRAGMA` 生成，不许手抄

实测 `market_v2` 的真实 DDL：

```sql
CREATE TABLE candles(symbol VARCHAR, date DATE, open DOUBLE, high DOUBLE NOT NULL,
  low DOUBLE NOT NULL, "close" DOUBLE NOT NULL, volume BIGINT NOT NULL,
  amount DOUBLE, turnover DOUBLE, PRIMARY KEY(symbol, date));
CREATE TABLE adj_factor(symbol VARCHAR, date DATE, factor DOUBLE NOT NULL, PRIMARY KEY(symbol, date));
CREATE TABLE shares_outstanding(symbol VARCHAR, date DATE, shares BIGINT NOT NULL, PRIMARY KEY(symbol, date));
CREATE TABLE symbols(symbol VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, market VARCHAR NOT NULL,
  status VARCHAR NOT NULL CHECK (status IN ('listed','delisted','suspended')));
```

- `open` / `amount` / `turnover` **可空**，其余 `NOT NULL`。`open` 可空是 `stock_tw` 的 v5 迁移专门改的（`0005_open_nullable.sql`），消费端有语义（`open IS NULL` 保留 bar，`close IS NULL` 才丢行）。**A 股库若把 `open` 回填 0 而不是 NULL，会静默破坏它。**
- `close` 是保留字，DDL 生成器必须加双引号。

#### 1.2 `adj_factor.factor` 的 qfq 语义写进文档字符串

见 1.5。**把 `cum` 直接写进 `factor` = 存成后复权**，对 2317 那种老股偏 `1/0.078 ≈ 13 倍`，**数值看着正常、不报任何错**。

#### 1.3 `schema_version` 语义写进注释

实测当前是 `[(1,),(2,),(3,),(4,),(5,)]` **五行**，是**迁移历史集合**而非单一版本号（`stock_tw/infrastructure/database.py:22`）。`PLAN.md:150` 只说「同构」，M2 照抄时容易猜错。

**影响文件**：新增 `src/market_data/markets/`、`src/market_data/markets/cn/` 两个包目录。

---

### 功能2: `src/market_data/markets/base.py`

#### 2.1 适配器接受 `base_url`，不含任何 SSH 逻辑

`FreeStockDbAdapter(base_url: str = "http://127.0.0.1:7899", ...)`，取数走普通 HTTP。

**理由**：把「经 SSH 拉取」写进适配器，等于让部署细节渗透整个数据层——本地测试时无法绕过，将来换传输方式要改数据层。正确做法是让这个特殊情况消失：**只管 URL，不管怎么到达**。

#### 2.2 `fetch` / `normalize` 两段式

`fetch(...) -> bytes`（只取数）、`normalize(raw) -> pd.DataFrame`（纯函数），用 `typing.Protocol`。

**收益**：`normalize` 可完全离线测，直接满足验收「不访问网络」。

#### 2.3 失败语义显式化

| 情况 | 异常 |
|---|---|
| 连接失败 / 超时 / 非 200 | `SourceUnavailable`（含原始响应体） |
| HTTP 200 + 空数组 | `SourceEmpty` —— **不是「无数据」**，参数写错也长这样，必须能被上层区分并告警 |
| msgpack 解得开但字段不全 | `SourceMalformed` |

---

### 功能3: `src/market_data/markets/cn/free_stockdb.py`

1. **加 `msgpack` 依赖**（`pyproject.toml`），`uv lock` 同步。
2. **查询构造**：按 1.2 实现 `k1` / `k2` / `num`。
3. **分块策略**：禁止 `qz:*&all:` 组合；按 **(标的, 全历史)** 或 **(前缀, 日期区间)** 分块并设上限——防 179MB 内存爆炸。
4. **四种返回形态都要解析**（1.3）。
5. **字段映射与类型规范化**：

   | 契约列 | 源字段 | 处理 |
   |---|---|---|
   | `symbol` | `code` | **源已是 str(6)**，直接透传；禁止数值转换 |
   | `date` | `date` | **uint32 `20260625`** → `DATE` |
   | `open/high/low/close` | 同名 | 不复权原值 |
   | `volume` | `volume` | **int vs float 都可能** → 显式 `int()`（股） |
   | `amount` | `amount` | **int vs float 都可能** → `float()`（元） |
   | `turnover` | `turnover` | `float()`，**百分数**，可能缺失 → NULL（**不是 0**） |

6. **单位断言**：`normalize` 之后强制跑 1.4 的两条；容差按源取整位数定（实测有 2 位与 4 位两种）。失败 → 抛异常、不产出 DataFrame。
7. **复权折算**：`cmd=get&t=复权&k1=key:<code>&k2=all:` → 解析日期 → 输出 `factor = cum / cum_latest`。
   ⚠️ `cum_latest` 会随新除权事件变化，**整条历史序列会平移**——M2 的写入必须能覆盖旧 factor 行（本 M1 只需产出正确的值）。
8. **代码表与 `symbols`**：`股票代码`（6 组 7563）+ `退市*`（918 条 → 339 个不同代码）→ `symbols(symbol, name, market, status)`。
   三个必须显式决定、不许猜的点：
   - **`market` 取值域**：按前缀推（`6→SH` / `0,3→SZ` / `9→BJ`）？还是与台股 `TWSE/TPEX` 统一？
   - **`status` 只有 3 个合法值**：`退市*` → `delisted` 直接；但**「停牌」在 A 股是每日状态、不是标的属性**，`suspended` 填什么需定义。填错会被 CHECK 拒（这是好事）。
   - **`name` 从哪取**：`日k` 每行带 `name`，但采自哪一天？`股票代码` 表拿不到名字。
9. **排序显式化**：`normalize` 出口按 `(symbol, date)` 升序。

---

### 功能4: `src/market_data/markets/cn/__init__.py`

稳定导出：适配器 + 契约常量，`__all__` 显式列出。M2/M3/M4 只从这一处 import。

---

## 四、测试计划

### 单元测试（**不访问网络、不启动 SSH**）

`tests/test_contracts.py`：
- 契约常量 vs `market_v2` 的 `PRAGMA table_info` 逐列一致（列名 / 类型 / 可空性）。
- 契约常量 vs `duckdb_constraints()` 的主键与 CHECK 一致。
- **`market_v2` 不存在时 `skipif` 且有明确原因**，不许静默跳过。
- 打开一律 `duckdb.connect(..., read_only=True)`。

`tests/test_free_stockdb_adapter.py`：
- **前导零**：`000001` 全链路保持 `"000001"`。
- **日期**：uint32 → `DATE`，`dtype` 断言。
- **数值类型**：int 与 float 两种 msgpack 编码都能转（用实测真实字节做 fixture）。
- **排序**：乱序 fixture → 输出有序。
- **空值**：`turnover` 缺失 → NULL 而非 0。
- **单位断言**：越界 fixture 必须被拒。
- **复权方向**：`cum` 序列 → `factor` 序列，末值 == 1.0。
- **四种返回形态**各一 fixture。
- **错误语义**：连接失败 → `SourceUnavailable`；200+`0x90` → `SourceEmpty`；字段缺失 → `SourceMalformed`。
- **分块器**：不产出超大请求（有断言）。

`tests/fixtures/free_stockdb/`（**存真实字节**，本次实测已取得可用样本）：
- 至少 4 只：沪市（`600633`，有 vendor 文档 + 实测双重对照）、深市、**前导零（`000001`）**、**退市股**
- 外加：`股票代码`、`退市*`、`复权`（含 `cum` 递增序列）、一条空数组响应、一条乱序响应

### 真实拉取验证（`@pytest.mark.network`，默认不跑）

- `600633` 近 5 个交易日逐字段比对（**已人工完成，固化为测试即可**）
- 覆盖深度抽样：随机 3 只 × 全历史

---

## 五、验收标准

- [ ] `contracts.py` 与 `market_v2` 的 `PRAGMA table_info` + `duckdb_constraints()` 逐列一致（有断言）。
- [ ] `adj_factor.factor` 标注为 **qfq**，且有「最新一日 == 1.0」断言。
- [ ] `pyproject.toml` 加了 `msgpack`，`uv lock` 一致。
- [ ] **适配器里没有任何 SSH 相关代码**；`base_url` 可配置，默认 `http://127.0.0.1:7899`。
- [ ] 用 fixture 能产出列名 / 类型 / 可空性完全符合契约的 DataFrame。
- [ ] `000001` 在标准化前后保持字符串原样。
- [ ] 单位断言强制执行，且有测试证明越界会被拒。
- [ ] `SourceUnavailable` / `SourceEmpty` / `SourceMalformed` 三者可区分。
- [ ] 分块器不产出超大请求（有断言）。
- [ ] 适配器单测不访问网络、不启动 SSH。
- [ ] `ruff format --check` + `ruff check` + `pyright` + `pytest` 全绿（先 `uv sync --extra alerts --extra dev`）。

---

## 六、兼容性检查

### 是否影响现有行为

- `offline/` 四件套**一行不动**，公开签名不变。
- CLI 现有 `manifest` / `verify-manifest` 语义不变（M1 不新增子命令）。
- **`market_v2.duckdb` 只读**：一致性测试必须 `read_only=True`，任何情况下不得写入。

### 消费端硬约束（定契约时必须提前知道，否则 M2 返工）

1. **`factor_research` 对 `adj_factor` 是 INNER JOIN**（`qlib/duckdb.py:29-37`）→ **缺一行 factor 就丢一行 K 线，且不报错**。契约注释要写明「candles 与 adj_factor 必须一一对应」。
2. 同一 SQL 有 `WHERE c.close > 0 AND c.volume > 0` → **A 股停牌（volume=0）整行会被过滤**。M3 的质量门禁**不要把停牌当坏数据拒掉**。
3. `amount` 为 NULL 时消费端填 `Decimal(0)`（不报错但污染因子）。A 股源有 amount，不该出现 NULL——值得一条断言。
4. **现成脚本的市场判定是 `symbol.isdigit() and len(symbol)==4 → TWSE else TPEX`**（`factor_research/scripts/batch_scan.py:35`）→ **A 股 6 位码会被全部判成 TPEX**。`PLAN.md:39` 的「研究代码零改动跨市场复用」**不成立**，调用方必须传新的 `market_resolver`。
5. `factor_research` **不读 `symbols` 表、不读 `schema_version`** → 功能3 第 8 条对**当前**消费者零收益。别算进「能被直接消费」的账上。

### 环境与 CI

- 本机 venv 没装 `lark_alert`（`uv sync --extra alerts` 从没跑过）→ `pytest` 收不了 `tests/test_alerts.py`，`pyright` 报 **18 个错**（全是该缺失的下游）。**这是环境问题不是代码问题。**
- `PLAN.md:92` 说「CI 覆盖 ruff/pyright/pytest」、`PLAN.md:163` 说「契约一致性测试是唯一防线，需在 CI 跑」——**本仓库既无 `.github/workflows/`，也无 git remote，防线不存在**。要么补 CI，要么从 PLAN 里删掉这句话。

### 文档债务

- `market_data/README.md` 与 `PLAN.md` 正面矛盾（还写着 `markets/cn`、`markets/tw`、Tushare、「leaf→termux 快照」），M1 落地后必须重写。
- `PLAN.md` / `DEPLOY.md` 按第二节清单更正。

---

## 七、明确不做（本轮）

- 不落库、不建 `market_a.duckdb`（M2）
- 不接质量门禁（M3）
- 不做 CLI 子命令、不做调度与续跑（M4）
- 不做 Tushare（本轮只用 free-stockdb）
- 不做分钟线（M5 后置）
- 不动 `offline/publish.py`（`PLAN.md:265` 判定无消费者；保留，README 重写时标注）
