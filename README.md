# market_data

统一的 A 股/台股离线数据管道。

> 当前状态：基础 manifest、质量门禁、原子发布和 lark_alert 告警接口已建立；A/TW 数据源适配与 leaf/termux 每日部署仍在实现中。

## 边界

```text
A股 Tushare / free-stockdb ─┐
                            ├─> market_data ─> leaf 主节点 ─> termux 副本
台股 FinMind / TWSE ────────┘
```

- `markets/cn`：A 股数据源与标准化入口；
- `markets/tw`：台股数据源与标准化入口；
- `offline/manifest.py`：快照文件数量、大小、SHA256 和最大日期；
- `offline/quality.py`：OHLCV、重复主键、日期和数值质量门禁；
- `offline/publish.py`：staging 完成后原子切换 `current`；
- `offline/alerts.py`：通过本地 `lark_alert` Python binding 发送去重告警。

## 设计原则

1. 失败更新不能覆盖上一个可用快照；
2. manifest 校验通过后才能发布；
3. leaf 是主节点，termux 先做副本和恢复节点，不做双写；
4. token、密码和 webhook secret 只从环境变量读取；
5. “文件已复制”不等于“数据可查询”，发布必须包含抽样查询和质量报告。

## 当前验证

项目要求 Python `>=3.12,<3.14`，验证必须使用项目自己的 `uv` 环境，不依赖系统 Python 或全局工具：

```bash
uv venv --python 3.12 .venv
uv sync --extra alerts --extra dev
uv lock --check
uv run python -V
uv run python -c "from lark_alert import Card, LarkAlert, Severity; print('lark_alert import ok')"
uv run --extra alerts --extra dev pytest tests -q
uv run --extra alerts --extra dev ruff check src tests
uv run --extra alerts --extra dev ruff format --check src tests
uv run --extra alerts --extra dev pyright
```
