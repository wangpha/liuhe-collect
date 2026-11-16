# 六合采集器 · 云端采集（公开仓库）

本仓库只放 **云端采集所需的二进制化核心** 与调度脚本。它每天在 GitHub Actions（免费额度）上
按当期最新期号抓取三个数据源、跑 OCR、产出结构化数据，再由本地软件增量拉取后生成报表。

## 仓库结构

```
cloud/
  runner.py        调度入口（--cleanup / --plan / --shard / --merge / --run / --selftest）
  _compat.py       Linux 适配垫片（强制 CPU OCR、假 windll、UTF-8）
  _period.py       期号四重校验（按时间排序 + isCurrPeriods + 单调性 + 开奖交叉）
  _reconcile.py    独立计数对账（帖子数 / 图片数）+ 21 产物清单
  _guard.py        多字段快照（防漏新增内容）+ 乱码扫描
  _cleanup.py      图片 2 天轮转 + 产物 30 期轮转
  audit_logs.py    运行日志零错误审计
  requirements.txt 依赖（与本地 1.0.128 安装版逐版本对齐）
  core/            【二进制】采集/OCR/报表核心，sourceless .pyc，无明文源码
.github/workflows/crawl.yml   采集流水线
```

## 红线（每次运行都校验，不通过即退出码非 0，不产出数据）

| 编号 | 红线 | 校验位置 |
|---|---|---|
| R11 | 绝不漏帖子 | `_reconcile.independent_scan_dahkg` + `reconcile_posts` |
| R12 | 绝不漏图片 | `reconcile_images`（逐帖 imgTotal vs 实际） |
| R13 | 绝不漏新增内容 | `_guard.build_snapshot` / `check_monotonic` |
| R14 | 绝不期数混乱 | `_period.detect_verified_dahkg` / `check_posts_period` |
| R15 | OCR 绝不认错 | 固定 rapidocr-onnxruntime 1.2.3 + 固定模型；缓存按图片 SHA256 命名 |

## 部署门禁（文档 6B）

Gate 1–5 全部通过 + 全量日志审计零命中之前，**不会启用 `schedule` 定时任务**。
`crawl.yml` 里的 `schedule` 段保持注释状态，只允许 `workflow_dispatch` 手动触发。

## 为什么是 .pyc

- 验收要求：公开仓库里 `grep -rn "def normalize_thread" .` 必须无结果；
- `.pyc` 是 sourceless 布局（旁边没有 `.py`），Python 3 用 `SourcelessFileLoader` 正常导入；
- 编译版本与 Actions 的 Python 版本锁死一致（3.13），magic number 不匹配会直接 ImportError（不会静默降级）。
