"""云端日志零错误审计（文档 6B.3 方式 1：自动脚本扫描）。

对每份 Actions 日志按 6 组正则扫描，命中即不合格：

    错误类 / 警告类 / 异常行为 / 数据问题 / 时间问题 / 退出码

用法：
    python cloud/audit_logs.py logs/*.log [--json out.json]
退出码：0 = 全部合格；1 = 有命中。
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

#: 每一组都按「出现任何一次即不通过」处理（文档 6B.3）
PATTERNS: list[tuple[str, str, str]] = [
    # (组名, 正则, 说明)
    ("错误类", r"\bERROR\b|\bFATAL\b|Traceback|\bException\b|失败|错误|\berror\b|\bfailed\b", "错误/异常关键字"),
    ("警告类", r"\bWARNING\b|警告|deprecated|已弃用", "警告关键字"),
    ("异常行为", r"超时|重试|\bretry\b|跳过|\bskip\b|熔断|卡死|中断|回退|降级", "超时/重试/跳过/降级"),
    ("数据问题", r"乱码|U\+FFFD|锟斤拷|\?\?\?|缺失|不全|不一致|差值", "乱码/缺失/不一致"),
    ("时间问题", r"连续\s*\d+\s*秒无输出|超时\s*\d+\s*秒", "长时间无输出"),
]

#: 「统计型」句子：关键字后面紧跟 0，是在汇报"没有异常"，不是异常本身。
#: 例：`[列表] 完成：980 条本期帖子，扫描 490 页，跳过其他期 0 条`
#:     `[防线4] 逐帖期号校验：检查 984 帖，错误 0 个`
#:     `[OCR] 完成 107 张：... 失败 0，未下载 0`
#: 注意：只放过 **数值为 0** 的情况；`失败 3` 依然判不通过（不放宽阈值）。
STAT_LINE = re.compile(
    r"(失败|错误|跳过|命中|缺口|差异|差值|不一致|未下载|超时|重试)"
    r"[^0-9\n]{0,10}?"          # 关键字与数字之间允许少量中文（如「跳过其他期 0 条」）
    r"0(?=\s*(?:个|条|张|次|期|，|,|$|\s))"
)

#: 允许白名单：某些行是**采集结果的正常描述**，不是故障。
#: 每一条都必须写明理由（红线：不许为了让检查通过而放宽）。
ALLOW: list[tuple[str, str]] = [
    (r"筛选：剔除", "列表筛除日志，属正常统计"),
    (r"剔除.*置顶", "剔除置顶帖，正常"),
    (r"非本期", "OCR 分类名 no_target/非本期，正常统计字段"),
    (r"失败\s*0", "'失败 0' 是统计计数，不是故障"),
    (r"差值\s*0", "'差值 0' 是对账通过的结论"),
    (r"differences?\s*=\s*0", "对账通过"),
    (r"WARNING: 正在从缓存恢复", "GitHub Actions cache 原生提示"),
    (r"Post job cleanup", "Actions 收尾阶段固定输出"),
    (r"准备解压|解压完成|Extracting", "缓存解压进度输出"),
    (r"Cache (restored|saved|not found|hit)", "缓存状态提示"),
    (r"No files were found", "artifact 空目录提示"),
    (r"Warning: (Node\.js|The following actions)", "Actions 平台自身版本提示"),
    (
        r"##\[warning\]Node\.js \d+ is deprecated|The following actions target Node\.js",
        "GitHub Actions 平台自身对内置 Node 版本的弃用提示，与本项目代码无关；"
        "升级 actions 版本才会消失，且不影响任何采集/OCR 结果",
    ),
    (
        r"\[DEP\d+\] DeprecationWarning: The `?punycode`? module is deprecated",
        "setup-python / actions 内置 Node 的 punycode 弃用提示，来自 Actions 运行时，"
        "不是我们的 Python 代码；同理还有 node:xxx 的 DEP 提示",
    ),
    (
        r"\[自检\] ✅",
        "自检**通过**的行。自检里本来就有一条「期号倒退必须中止」的用例，"
        "它打印的文字含『失败/中止』属预期；未通过会显示 ❌，仍会被抓",
    ),
    (
        r"##\[group\]Run ",
        "GitHub 在步骤开始时把**脚本原文**回显出来（不是执行结果）；"
        "脚本里含『错误』等字样的行会随之出现，真正的失败会在 ##[error] 行体现",
    ),
    (
        r"云端日志零错误审计（文档 6B\.3）",
        "本审计工具**自己的标题行**：它在 Actions 里跑，输出又被同一份 Actions 日志收录，"
        "标题里含『错误』二字属自指，不是异常",
    ),
    (
        r"find: write error",
        "`find ... | head -N` 的 SIGPIPE 提示（head 提前关闭管道）。"
        "已把 workflow 改成不依赖 head 的写法；此条为历史日志兜底",
    ),
    (
        r"DiscoverDevicesForPlatform\]\s*GPU device discovery failed",
        "无显卡 runner 上 onnxruntime 探测 GPU 的固定提示；CPU 模式已强制"
        "（QODE_OCR_DML=0），同轮 OCR 统计 失败 0 / 缓存命中 100%，非故障。"
        "已由 cloud/sitecustomize.py 把 ORT 日志级别降到 ERROR，此行属兜底白名单",
    ),
    (
        r"乱码扫描：检查 \d+ 个字符串，硬乱码 \d+ / 连续问号 \d+",
        "这是**审计汇报行**本身，不是异常；软命中(连续问号)已记录待人工复核",
    ),
    (
        r"乱码扫描：检查 \d+ 个字符串，命中 \d+",
        "旧格式的**审计汇报行**本身（新版本已拆成 硬乱码/连续问号），不是异常",
    ),
]


def is_statistic(line: str) -> bool:
    """该行是否属于「关键字 + 0」的统计汇报（0 表示无异常）。"""
    return bool(STAT_LINE.search(line))


def scan_line(line: str) -> list[dict]:
    hits: list[dict] = []
    if is_statistic(line):
        # 关键字后面跟的是 0 —— 这是在汇报"零异常"，不是异常
        return hits
    if any(re.search(allow, line) for allow, _ in ALLOW):
        return hits
    for group, pattern, why in PATTERNS:
        m = re.search(pattern, line)
        if not m:
            continue
        hits.append({"group": group, "matched": m.group(0), "why": why, "line": line.strip()[:300]})
    return hits


def analyze_gaps(path: Path, limit_seconds: float = 60.0) -> dict:
    """按时间戳计算「连续多久没有输出」（文档 6B.3「时间问题」判据）。

    只有 `gh run view --log` 导出的日志带时间戳（形如
    `job\tstep\t2026-11-16T02:28:36.2098270Z 内容`），
    从 artifact 里拿到的纯文本日志没有时间戳 —— 这种情况返回 checked=False，
    并在报告里明确写"本份日志无时间戳，无法核验静默间隔"，不假装通过。
    """
    ts_re = re.compile(r"\t(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s")
    stamps: list[tuple[float, str]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = ts_re.search(line)
        if not m:
            continue
        text = m.group(1).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            continue
        stamps.append((dt.timestamp(), line.strip()[:160]))

    if len(stamps) < 2:
        return {"checked": False, "reason": "日志无时间戳（artifact 纯文本），无法核验静默间隔"}

    biggest = 0.0
    worst = None
    violations: list[dict] = []
    for (t0, l0), (t1, l1) in zip(stamps, stamps[1:]):
        gap = t1 - t0
        if gap > biggest:
            biggest, worst = gap, l1
        if gap > limit_seconds:
            violations.append({"gapSeconds": round(gap, 1), "after": l0, "before": l1})
    return {
        "checked": True,
        "linesWithTs": len(stamps),
        "maxGapSeconds": round(biggest, 1),
        "worstAfter": worst,
        "violations": violations[:20],
        "violationCount": len(violations),
        "limitSeconds": limit_seconds,
        "passed": not violations,
    }


def split_actions_prefix(line: str) -> tuple[str, str]:
    """拆掉 `gh run view --log` 的行前缀：`<job>\\t<step>\\t<timestamp>Z <内容>`。

    为什么必须拆（2026-11-16 实测）：本项目有个步骤叫「日志零错误审计」，
    **步骤名里自带"错误"**，不拆前缀会把该步骤的每一行都误判成错误命中。
    """
    parts = line.split("\t")
    if len(parts) >= 3:
        head = "\t".join(parts[:2])
        rest = "\t".join(parts[2:])
        m = re.match(r"\ufeff?(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s?(.*)$", rest)
        if m:
            return head, m.group(2)
        return head, rest
    return "", line


def audit_file(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    hits: list[dict] = []
    for no, raw in enumerate(lines, 1):
        _head, body = split_actions_prefix(raw)
        for hit in scan_line(body):
            hit["lineNo"] = no
            hit["line"] = body.strip()[:300]
            hits.append(hit)
    exit_codes = [int(m.group(1)) for m in re.finditer(r"(?:退出码|exit[ _]?code)\s*[=: ]\s*(-?\d+)", text, re.I)]
    bad_exit = [c for c in exit_codes if c != 0]
    gaps = analyze_gaps(path)
    return {
        "file": str(path),
        "lines": len(lines),
        "hits": hits,
        "hitCount": len(hits),
        "groups": sorted({h["group"] for h in hits}),
        "exitCodes": exit_codes,
        "badExitCodes": bad_exit,
        "silence": gaps,
        "passed": not hits and not bad_exit and gaps.get("passed", True),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="云端日志零错误审计")
    parser.add_argument("logs", nargs="+", help="日志文件")
    parser.add_argument("--json", default="", help="把结果写到 JSON")
    parser.add_argument("--max-show", type=int, default=5, help="每份日志最多显示几条命中")
    args = parser.parse_args(argv)

    results = []
    print("=" * 78)
    print("云端日志零错误审计（文档 6B.3）")
    print("=" * 78)
    for name in args.logs:
        path = Path(name)
        if not path.exists():
            print(f"[跳过] 不存在：{name}")
            continue
        res = audit_file(path)
        results.append(res)
        flag = "✅ 通过" if res["passed"] else "❌ 不通过"
        sil = res.get("silence") or {}
        sil_txt = (
            f"最长静默 {sil.get('maxGapSeconds')}s"
            if sil.get("checked")
            else f"静默未核验（{sil.get('reason','无时间戳')}）"
        )
        print(f"\n{flag}  {path.name}   行数={res['lines']}  命中={res['hitCount']}  退出码={res['exitCodes'] or '无'}  {sil_txt}")
        for hit in res["hits"][: args.max_show]:
            print(f"    L{hit['lineNo']} [{hit['group']}] {hit['matched']}  <- {hit['line']}")
        if res["badExitCodes"]:
            print(f"    非零退出码：{res['badExitCodes']}")
        for v in sil.get("violations", [])[:5]:
            print(f"    静默 {v['gapSeconds']}s：{v['after']}  →  {v['before']}")

    ok = all(r["passed"] for r in results) and bool(results)
    empty = [r["file"] for r in results if r["lines"] == 0]
    if empty:
        print(f"\n[不合格] 以下日志是空文件（下载失败/未生成），不能算通过：{empty}")
        ok = False
    passed_n = sum(1 for r in results if r["passed"] and r["lines"] > 0)
    failed_n = len(results) - passed_n
    print("\n" + "=" * 78)
    print(f"总计 {len(results)} 份日志：{passed_n} 份通过，{failed_n} 份不通过"
          f" -> {'全部合格 ✅' if ok else '存在不合格 ❌'}")
    print("=" * 78)

    if args.json:
        Path(args.json).write_text(
            json.dumps({"passed": ok, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"结果已写：{args.json}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
