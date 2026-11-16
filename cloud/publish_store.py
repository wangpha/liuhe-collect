"""把各源产物写进私有数据仓（每期一个 .tar.gz）+ 生成 manifest。

## 为什么不用「每期目录平铺 17 个文件」
2026-11-16 实测：大港澳一期（992 帖）users.json **209 MB**、posts.json **191 MB**，
GitHub 单文件上限 100 MB → push 被 pre-receive hook 拒绝：
    remote: error: File .../users.json is 209.18 MB; exceeds GitHub's file size limit of 100.00 MB
    ! [remote rejected] main -> main (pre-receive hook declined)
文档 3.5 表格里的 "users.json 16.35 MB" 是过时数据（本地 562 帖时已是 90 MB）。

改成每期一个 tar.gz：
  · 实测压缩比 13.6 倍（244 MB → 17.2 MB），既绕开限制又大幅减小仓库增长
  · 解包后与本地产物目录逐字节一致（cloud/pack_period.py 保证，已自检）
  · manifest 记录每期的 sha256 / 文件数 / 压缩后大小，本地 sync 据此校验

用法：
    python cloud/publish_store.py --incoming work/incoming --store work/store
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pack_period import pack, unpack  # noqa: E402

NAME_MAP = {"大港澳": "dahkg", "澳彩交流": "aocai", "tuku": "tuku", "澳门": "tuku"}
ARCHIVE_SUFFIX = ".tar.gz"


def key_for(artifact_name: str, period_dir: Path) -> str:
    parts = artifact_name.split("-")
    if len(parts) >= 2 and parts[1] in ("dahkg", "aocai", "tuku"):
        return parts[1]
    return NAME_MAP.get(period_dir.parent.name, period_dir.parent.name or "unknown")


def publish(incoming: Path, store: Path, log=print) -> dict:
    incoming = Path(incoming)
    store = Path(store)
    data = store / "data"
    data.mkdir(parents=True, exist_ok=True)

    published: list[dict] = []
    for src_root in sorted(p for p in incoming.glob("products-*") if p.is_dir()):
        found = [p for p in src_root.rglob("*期") if p.is_dir() and (p / "posts.json").exists()]
        skipped = [p for p in src_root.rglob("*期") if p.is_dir() and not (p / "posts.json").exists()]
        log(f"{src_root.name}: 正式期目录 {len(found)} 个 -> {[p.name for p in found][:5]}")
        if skipped:
            log(f"{src_root.name}: 跳过非产物目录 {len(skipped)} 个")
        for period_dir in found:
            key = key_for(src_root.name, period_dir)
            dst_dir = data / key
            dst_dir.mkdir(parents=True, exist_ok=True)
            archive = dst_dir / f"{period_dir.name}{ARCHIVE_SUFFIX}"
            info = pack(period_dir, archive)
            # 清掉同期的旧「平铺目录」写法，避免两种形态并存
            legacy = dst_dir / period_dir.name
            if legacy.is_dir():
                shutil.rmtree(legacy)
                log(f"  已删除旧平铺目录：{legacy}")
            log(
                f"  打包 {key}/{period_dir.name}: {info['files']} 文件，"
                f"{info['rawBytes']/1024/1024:.1f} MB -> {info['archiveBytes']/1024/1024:.1f} MB"
                f"（sha256 {info['sha256'][:16]}…）"
            )
            published.append({"key": key, **info})

    if not published:
        log("FATAL: 没有把任何一期的产物写进数据仓")
        return {"published": [], "manifest": {}, "ok": False}

    manifest = {}
    for key_dir in sorted(p for p in data.iterdir() if p.is_dir()):
        entries = {}
        for archive in sorted(key_dir.glob(f"*{ARCHIVE_SUFFIX}")):
            period = archive.name[: -len(ARCHIVE_SUFFIX)]
            info = next((x for x in published if x["key"] == key_dir.name and x["period"] == period), {})
            entries[period] = {
                "archive": archive.name,
                "archiveBytes": archive.stat().st_size,
                "files": info.get("files", 0),
                "rawBytes": info.get("rawBytes", 0),
                "sha256": info.get("sha256") or _sha256(archive),
            }
        periods = sorted(entries, key=_period_no)
        manifest[key_dir.name] = {"periods": periods, "count": len(periods), "entries": entries}

    store.mkdir(parents=True, exist_ok=True)
    (store / "manifest.json").write_text(
        json.dumps(
            {
                "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "layout": "one-tar-gz-per-period",
                "note": "每期一个 .tar.gz；解包后为 21/22 个产物（不含 images/.cache）。"
                        "用 cloud/pack_period.py unpack 解包并校验 sha256。",
                "sources": manifest,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log(f"manifest 已更新：{ {k: v['count'] for k, v in manifest.items()} }")
    return {"published": published, "manifest": manifest, "ok": True}


def _period_no(name: str) -> int:
    digits = "".join(c for c in name.split("_")[-1] if c.isdigit())
    return int(digits or 0)


def _sha256(path: Path) -> str:
    import hashlib

    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def selftest() -> int:
    import tempfile

    tmp = Path(tempfile.mkdtemp(prefix="pubstore-"))
    inc = tmp / "incoming" / "products-dahkg-1"
    pd = inc / "大港澳" / "澳门_2026_320期"
    pd.mkdir(parents=True)
    (pd / "posts.json").write_text('{"x":1}', encoding="utf-8")
    (pd / "users.json").write_text('{"y":2}', encoding="utf-8")
    # 造一个旧的平铺目录，验证会被清掉
    legacy = tmp / "store" / "data" / "dahkg" / "澳门_2026_320期"
    legacy.mkdir(parents=True)
    (legacy / "posts.json").write_text("old", encoding="utf-8")

    res = publish(tmp / "incoming", tmp / "store", log=print)
    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + msg)
        ok = ok and cond

    arc = tmp / "store" / "data" / "dahkg" / "澳门_2026_320期.tar.gz"
    man = json.loads((tmp / "store" / "manifest.json").read_text(encoding="utf-8"))
    print("\n自检：")
    check(res["ok"], "publish 成功")
    check(arc.is_file(), f"应生成 {arc.name}")
    check(not legacy.exists(), "旧平铺目录应被删除")
    check(man["sources"]["dahkg"]["count"] == 1, "manifest 记 1 期")
    entry = man["sources"]["dahkg"]["entries"]["澳门_2026_320期"]
    check(entry["sha256"] == res["published"][0]["sha256"], "manifest 里的 sha256 与实际一致")
    out = tmp / "restore" / "澳门_2026_320期"
    r = unpack(arc, out, entry["sha256"], 2)
    check(r["passed"] and (out / "posts.json").read_text(encoding="utf-8") == '{"x":1}', "解包可还原原内容")
    shutil.rmtree(tmp, ignore_errors=True)
    print("结论：" + ("自检通过 ✅" if ok else "自检失败 ❌"))
    return 0 if ok else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--incoming", help="下载下来的 products-* 目录")
    ap.add_argument("--store", help="私有数据仓检出目录")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return selftest()
    if not args.incoming or not args.store:
        ap.error("需要 --incoming 与 --store")
    res = publish(Path(args.incoming), Path(args.store))
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

