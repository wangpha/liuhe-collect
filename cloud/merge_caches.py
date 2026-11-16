"""分片缓存合并器：把 N 个分片的 `.cache` 合并成一份。

## 为什么必须专门写（2026-11-16 实测）
分片 job 各自跑一遍模块，会在同一路径写出 `details.json` / `replies.json` /
`threads.json` / `list.json`。若在 merge job 里把 N 个 tar 都解到同一目录，
**后解的会把先解的覆盖掉** —— merge 只能拿到 1/N 的缓存，于是：
  · 详情/回复要重新抓 → 网络请求暴涨 → 出现大量 502 重试
  · 实测 Gate1 分片版：重试 44 条、OCR 单批失败 9 行（串行版只有 2 条/0 行）

## 合并规则（按实测 schema 写，不猜）
| 文件 | schema | 合并方式 |
|---|---|---|
| `details.json` | `{period, year, details:{aid: entry}}` | `details` 按 aid 取并集 |
| `replies.json` | `{version, period, year, posts:{aid: [...]}}` | `posts` 按 aid 取并集 |
| `threads.json` | `{version, threads:{tid: {...}}}` | `threads` 按 tid 取并集 |
| `list.json` | `{period, year, rows:[...]}` | `rows` 按 articleId 去重取并集 |
| `ocr/*` | 每张图一个文件，**按图片 SHA256 命名** | 按文件名复制（内容寻址，天然不冲突） |
| 其他 | — | 目标里没有就复制 |

用法：
    python cloud/merge_caches.py --target work/out --sources work/cache-in
    python cloud/merge_caches.py --selftest        # 用合成数据验证并集逻辑
"""
from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

#: 需要按 key 合并的 JSON：文件名 -> (容器键, 字段键)
DICT_MERGES = {
    "details.json": "details",
    "replies.json": "posts",
    "threads.json": "threads",
}
LIST_MERGES = {"list.json": ("rows", "articleId")}


def _load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _merge_dict_file(target: Path, sources: list[Path], key: str) -> int:
    merged: dict = {}
    base: dict = {}
    for src in sources:
        data = _load(src)
        if not isinstance(data, dict):
            continue
        if not base:
            base = {k: v for k, v in data.items() if k != key}
        bucket = data.get(key)
        if isinstance(bucket, dict):
            merged.update(bucket)
    if not merged and not base:
        return 0
    existing = _load(target) if target.exists() else None
    if isinstance(existing, dict):
        if not base:
            base = {k: v for k, v in existing.items() if k != key}
        old = existing.get(key)
        if isinstance(old, dict):
            merged.update(old)
    out = dict(base)
    out[key] = merged
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    return len(merged)


def _merge_list_file(target: Path, sources: list[Path], key: str, id_field: str) -> int:
    seen: dict = {}
    base: dict = {}
    for src in sources:
        data = _load(src)
        if not isinstance(data, dict):
            continue
        if not base:
            base = {k: v for k, v in data.items() if k != key}
        for row in data.get(key) or []:
            if isinstance(row, dict):
                rid = row.get(id_field)
                if rid is not None:
                    seen[rid] = row
    existing = _load(target) if target.exists() else None
    if isinstance(existing, dict):
        for row in existing.get(key) or []:
            if isinstance(row, dict):
                rid = row.get(id_field)
                if rid is not None and rid not in seen:
                    seen[rid] = row
    if not seen and not base:
        return 0
    out = dict(base)
    out[key] = list(seen.values())
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    return len(seen)


def merge_caches(target_root: Path, source_roots: list[Path], log=print) -> dict:
    """把 source_roots 下所有 `*/.cache/**` 合并进 target_root 的对应路径。"""
    target_root = Path(target_root)
    # 先把「同一个 .cache 目录」在多份来源里的同一文件名归到一组
    groups: dict[tuple[Path, str], list[Path]] = {}
    copied_ocr = 0
    copied_other = 0

    for root in source_roots:
        root = Path(root)
        if not root.is_dir():
            continue
        for cache_dir in [p for p in root.rglob(".cache") if p.is_dir()]:
            rel_parent = cache_dir.parent.relative_to(root)      # 例如 澳门_2026_320期
            for path in cache_dir.rglob("*"):
                if not path.is_file():
                    continue
                rel_inside = path.relative_to(cache_dir)          # 例如 ocr/abc.json
                key = (target_root / rel_parent / ".cache", str(rel_inside))
                if path.name == ".write_probe":
                    continue
                if path.parent.name == "ocr":
                    dst = target_root / rel_parent / ".cache" / rel_inside
                    if not dst.exists():
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(path, dst)
                        copied_ocr += 1
                else:
                    groups.setdefault(key, []).append(path)

    stat_lines = []
    for (parent, name), srcs in sorted(groups.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
        target = Path(parent) / Path(name).name if False else Path(parent) / name
        if Path(name).name in DICT_MERGES:
            n = _merge_dict_file(target, srcs, DICT_MERGES[Path(name).name])
            stat_lines.append(f"{parent}/{name}: 合并 {n} 条（来自 {len(srcs)} 份分片）")
        elif Path(name).name in LIST_MERGES:
            key, id_field = LIST_MERGES[Path(name).name]
            n = _merge_list_file(target, srcs, key, id_field)
            stat_lines.append(f"{parent}/{name}: 合并 {n} 行（来自 {len(srcs)} 份分片）")
        else:
            best = max(srcs, key=lambda p: p.stat().st_size)
            if not target.exists() or target.stat().st_size < best.stat().st_size:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(best, target)
                copied_other += 1

    for line in stat_lines:
        log(f"[合并缓存] {line}")
    log(f"[合并缓存] OCR 文件复制 {copied_ocr} 个；其他文件复制 {copied_other} 个")
    return {
        "ocrCopied": copied_ocr,
        "otherCopied": copied_other,
        "mergedFiles": len(stat_lines),
        "sources": len(source_roots),
    }


# ---------------------------------------------------------------------------
# 自检：合成两份分片缓存，验证并集正确
# ---------------------------------------------------------------------------
def selftest() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="cachemerge-"))
    tgt = tmp / "out"
    s1 = tmp / "shard1" / "澳门_2026_320期" / ".cache"
    s2 = tmp / "shard2" / "澳门_2026_320期" / ".cache"
    for d in (s1, s2, tgt / "澳门_2026_320期" / ".cache"):
        d.mkdir(parents=True, exist_ok=True)

    (s1 / "details.json").write_text(json.dumps({"period": 320, "year": 2026, "details": {"1": {"t": "a"}, "2": {"t": "b"}}}), encoding="utf-8")
    (s2 / "details.json").write_text(json.dumps({"period": 320, "year": 2026, "details": {"2": {"t": "b"}, "3": {"t": "c"}}}), encoding="utf-8")
    (s1 / "replies.json").write_text(json.dumps({"version": 1, "period": 320, "posts": {"1": [1]}}), encoding="utf-8")
    (s2 / "replies.json").write_text(json.dumps({"version": 1, "period": 320, "posts": {"2": [2]}}), encoding="utf-8")
    (s1 / "list.json").write_text(json.dumps({"period": 320, "rows": [{"articleId": 1}, {"articleId": 2}]}), encoding="utf-8")
    (s2 / "list.json").write_text(json.dumps({"period": 320, "rows": [{"articleId": 2}, {"articleId": 3}]}), encoding="utf-8")
    (s1 / "ocr").mkdir(parents=True, exist_ok=True)
    (s2 / "ocr").mkdir(parents=True, exist_ok=True)
    (s1 / "ocr" / "a.json").write_text("{}", encoding="utf-8")
    (s2 / "ocr" / "b.json").write_text("{}", encoding="utf-8")

    res = merge_caches(tgt, [tmp / "shard1", tmp / "shard2"], log=print)
    d = json.loads((tgt / "澳门_2026_320期" / ".cache" / "details.json").read_text(encoding="utf-8"))
    r = json.loads((tgt / "澳门_2026_320期" / ".cache" / "replies.json").read_text(encoding="utf-8"))
    li = json.loads((tgt / "澳门_2026_320期" / ".cache" / "list.json").read_text(encoding="utf-8"))
    ocr = sorted(p.name for p in (tgt / "澳门_2026_320期" / ".cache" / "ocr").iterdir())

    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + msg)
        ok = ok and cond

    print("\n自检：")
    check(sorted(d["details"]) == ["1", "2", "3"], f"details 并集应为 1/2/3，实际 {sorted(d['details'])}")
    check(sorted(r["posts"]) == ["1", "2"], f"replies 并集应为 1/2，实际 {sorted(r['posts'])}")
    check(sorted(x["articleId"] for x in li["rows"]) == [1, 2, 3], f"list 去重并集应为 1/2/3，实际 {sorted(x['articleId'] for x in li['rows'])}")
    check(ocr == ["a.json", "b.json"], f"OCR 应两份都在，实际 {ocr}")
    check(res["ocrCopied"] == 2, f"OCR 复制数应为 2，实际 {res['ocrCopied']}")
    shutil.rmtree(tmp, ignore_errors=True)
    print("\n结论：" + ("自检通过 ✅" if ok else "自检失败 ❌"))
    return 0 if ok else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="分片缓存合并器")
    ap.add_argument("--target", help="目标输出根（work/out）")
    ap.add_argument("--sources", nargs="+", help="分片缓存根目录列表")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return selftest()
    if not args.target or not args.sources:
        ap.error("需要 --target 与 --sources")
    res = merge_caches(Path(args.target), [Path(s) for s in args.sources])
    print(json.dumps(res, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
