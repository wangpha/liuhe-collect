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
]


def scan_line(line: str) -> list[dict]:
    hits: list[dict] = []
    for group, pattern, why in PATTERNS:
        m = re.search(pattern, line)
        if not m:
            continue
        if any(re.search(allow, line) for allow, _ in ALLOW):
            continue
        hits.append({"group": group, "matched": m.group(0), "why": why, "line": line.strip()[:300]})
    return hits


def audit_file(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    hits: list[dict] = []
    for no, line in enumerate(lines, 1):
        for hit in scan_line(line):
            hit["lineNo"] = no
            hits.append(hit)
    exit_codes = [int(m.group(1)) for m in re.finditer(r"(?:退出码|exit[ _]?code)\s*[=: ]\s*(-?\d+)", text, re.I)]
    bad_exit = [c for c in exit_codes if c != 0]
    return {
        "file": str(path),
        "lines": len(lines),
        "hits": hits,
        "hitCount": len(hits),
        "groups": sorted({h["group"] for h in hits}),
        "exitCodes": exit_codes,
        "badExitCodes": bad_exit,
        "passed": not hits and not bad_exit,
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
        print(f"\n{flag}  {path.name}   行数={res['lines']}  命中={res['hitCount']}  退出码={res['exitCodes'] or '无'}")
        for hit in res["hits"][: args.max_show]:
            print(f"    L{hit['lineNo']} [{hit['group']}] {hit['matched']}  <- {hit['line']}")
        if res["badExitCodes"]:
            print(f"    非零退出码：{res['badExitCodes']}")

    ok = all(r["passed"] for r in results) and bool(results)
    print("\n" + "=" * 78)
    print(f"总计 {len(results)} 份日志：{sum(1 for r in results if r['passed'])} 份通过，"
          f"{sum(1 for r in results if not r['passed'])} 份不通过 -> {'全部合格 ✅' if ok else '存在不合格 ❌'}")
    print("=" * 78)

    if args.json:
        Path(args.json).write_text(
            json.dumps({"passed": ok, "results": results}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"结果已写：{args.json}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
