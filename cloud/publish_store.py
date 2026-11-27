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

# 档位判定统一用北京时间（与 workflow 里 `TZ=Asia/Shanghai date` 完全一致）。
# 不能依赖 runner 的本地时区：GitHub Actions 是 UTC，
# 而 `TZ=Asia/Shanghai` 在 gate 里被显式指定。
_BEIJING_TZ = None
try:
    from datetime import timedelta, timezone as _timezone

    _BEIJING_TZ = _timezone(timedelta(hours=8), name="Asia/Shanghai")
except Exception:  # noqa: BLE001 - 极端情况下退回 time
    _BEIJING_TZ = None


def _beijing_ymd() -> str:
    """今天（北京时区）的 YYYY-MM-DD，与 gate 的 `TZ=Asia/Shanghai date '+%Y-%m-%d'` 对齐。"""
    if _BEIJING_TZ is not None:
        from datetime import datetime

        return datetime.now(_BEIJING_TZ).strftime("%Y-%m-%d")
    # 兜底：老版本 Python 无 tzinfo 支持时用 UTC+8 手工换算
    return time.strftime("%Y-%m-%d", time.gmtime(time.time() + 8 * 3600))

NAME_MAP = {"大港澳": "dahkg", "澳彩交流": "aocai", "tuku": "tuku", "澳门": "tuku"}
ARCHIVE_SUFFIX = ".tar.gz"


def key_for(artifact_name: str, period_dir: Path) -> str:
    parts = artifact_name.split("-")
    if len(parts) >= 2 and parts[1] in ("dahkg", "aocai", "tuku"):
        return parts[1]
    return NAME_MAP.get(period_dir.parent.name, period_dir.parent.name or "unknown")


def publish(incoming: Path, store: Path, log=print, slot: str = "") -> dict:
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
            # ⚠ 2026-11-22 根因修复（实测数据仓 320-325 期 files=0 rawBytes=0）：
            #   原来对「本轮未重新打包的期」default 成 0，导致 manifest 元数据缺失。
            #   修法：没有本轮 info 时，从 tar.gz **实测**文件数与解压字节数。
            if info.get("files") and info.get("rawBytes"):
                _files = int(info["files"])
                _raw = int(info["rawBytes"])
            else:
                _files, _raw = _archive_stats(archive)
            entries[period] = {
                "archive": archive.name,
                "archiveBytes": archive.stat().st_size,
                "files": _files,
                "rawBytes": _raw,
                "sha256": info.get("sha256") or _sha256(archive),
            }
        periods = sorted(entries, key=_period_no)
        manifest[key_dir.name] = {"periods": periods, "count": len(periods), "entries": entries}

    store.mkdir(parents=True, exist_ok=True)
    # ⚠ 2026-11-24 根因修复（实测：今天五档只完成 1 档，其余 4 档永不补跑）：
    #
    #   门控（JobG）的自愈补跑原来只用「manifest.generatedAt ≥ 该档时间」
    #   判断「该档跑过了」：
    #       if [ "$LAST_MIN_OF_DAY" -ge "$sm" ]; then _ran_this=1; fi
    #   但这个判据是**错的**：
    #       · generatedAt 只是「最后一次 publish 的时间」，不记录「跑过哪几档」
    #       · 实测 23:24 发布后 generatedAt=23:24
    #         → 15:00/18:00/20:10/20:40 四档全被判「跑过了」→ 永远不补跑
    #       · 今天真实结果：五档只完成 12:00 一档，其余四档数据永久缺失
    #
    #   修法：manifest 里新增 `completedSlots`（当天已完成的档位列表），
    #         格式 {"date": "2026-11-24", "slots": ["12:00", ...]}
    #         门控据此做**精确**的「该档跑过没」判断，而不是靠时间推导。
    #   `slot` 参数由 publish job 从 gate.outputs.slot 传进来。
    _prev_slots: dict = {}
    try:
        _prev_path = store / "manifest.json"
        if _prev_path.exists():
            _prev = json.loads(_prev_path.read_text(encoding="utf-8"))
            if isinstance(_prev.get("completedSlots"), dict):
                _prev_slots = dict(_prev["completedSlots"])
    except Exception:  # noqa: BLE001 - 旧 manifest 读不出来就当空的
        _prev_slots = {}

    # ⚠ 2026-11-25 根因修复（时区错配）：gate 用 `TZ=Asia/Shanghai date` 取「北京日期」，
    #   而这里原来用 `time.strftime("%Y-%m-%d")` 取的是 **runner 的本地时区（UTC）**。
    #   → 北京时间 00:00-08:00 期间发布时，UTC 还在前一天，
    #     `completedSlots.date` 会比 gate 的 NOW_YMD 早一天 → 自愈补跑判据永不匹配。
    #   实测：`completedSlots = {"date":"2026-11-24","slots":["20:40"]}`
    #        那次是 UTC 17:30 发布 = 北京 25 日 01:30 → 日期确实错了。
    #   修法：统一用北京时区取日期（与 gate 完全一致）。
    _today = _beijing_ymd()
    _slot_name = str(slot or "").strip()
    # 只保留当天；跨天自动清零
    if _prev_slots.get("date") != _today:
        _prev_slots = {"date": _today, "slots": []}
    _slot_list = list(_prev_slots.get("slots") or [])
    # ⚠ 2026-11-27 根因修复：completedSlots 被「非五档」的 slot 污染。
    #
    #   原判据只排除 manual/busy/none 三个字面值，其它任何字符串都会被当成合法档位写进去。
    #   实测：闹钟自测（test_slot=09:32）触发 crawl 时带 slot=09:32，
    #   gate 对「手动/外部触发」直接放行 → publish 把 "09:32" 写进了 completedSlots
    #   → 数据仓台账出现一个根本不存在的档位，且永久留存（跨天才清）。
    #
    #   修法（两层）：
    #     ① 只认五档白名单，不在名单里的 slot 一律不记录（源头堵住）
    #     ② 顺手把台账里已有的非五档条目清掉（治理存量脏数据）
    #   gate 的自愈补跑是「遍历五档、逐个查名字是否在列表里」，
    #   假档位不会被误匹配，但会让「今天跑过哪些档」的记录失真，所以要治。
    _SLOT_WHITELIST = ("12:00", "15:00", "18:00", "20:10", "20:40")
    # ② 治理存量：清掉历史混入的非五档条目
    _before_clean = len(_slot_list)
    _slot_list = [s for s in _slot_list if str(s) in _SLOT_WHITELIST]
    if len(_slot_list) != _before_clean:
        log(f"[publish] 🧹 completedSlots 清掉 {_before_clean - len(_slot_list)} 个非五档条目")
    # ① 源头：只有五档才记录
    #    ⚠ 顺序不能反：先去掉「(补跑)」后缀，再比对白名单。
    #    gate 的自愈补跑传进来的是「12:00(补跑)」，若先比白名单会被整个拒掉
    #    → 补跑成功的档位记不进台账 → 下次又被判没跑 → 再补 → 无穷循环。
    #    （单测 _test_slot_whitelist.py「带(补跑)后缀 归一化」当场抓到的回归）
    _clean = _slot_name.split("(")[0].strip()
    if _clean and _clean in _SLOT_WHITELIST:
        if _clean not in _slot_list:
            _slot_list.append(_clean)
            _slot_list.sort()
    elif _slot_name and _slot_name not in ("manual", "busy", "none"):
        log(f"[publish] ⚠ slot={_slot_name!r} 不在五档白名单 → 不写入 completedSlots")
    _prev_slots["slots"] = _slot_list

    (store / "manifest.json").write_text(
        json.dumps(
            {
                "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "layout": "one-tar-gz-per-period",
                "note": "每期一个 .tar.gz；解包后为 21/22 个产物（不含 images/.cache）。"
                        "用 cloud/pack_period.py unpack 解包并校验 sha256。",
                # ⚠ 当天已完成的档位（门控自愈补跑据此判断，不再用时间推导）
                "completedSlots": _prev_slots,
                "sources": manifest,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log(
        f"manifest 已更新：{ {k: v['count'] for k, v in manifest.items()} }"
        f"；今日已完成档位={_slot_list}"
    )
    return {"published": published, "manifest": manifest, "ok": True}


def _period_no(name: str) -> int:
    digits = "".join(c for c in name.split("_")[-1] if c.isdigit())
    return int(digits or 0)


def _archive_stats(archive: Path) -> tuple[int, int]:
    """从 tar.gz 实测 (文件数, 解压后字节数)。

    ## 为什么需要（2026-11-22 根因修复）
    manifest 每轮只填「本轮重新打包的期」的 files/rawBytes，
    其余期 default 成 0 —— 实测数据仓 320-325 期全部 files=0。
    这里从归档文件本身实测，保证每一期都有真实元数据。
    """
    import tarfile

    files = 0
    raw = 0
    try:
        with tarfile.open(archive, "r:gz") as t:
            for m in t.getmembers():
                if m.isdir():
                    continue
                files += 1
                raw += int(m.size or 0)
    except (OSError, tarfile.TarError):
        return (0, 0)
    return (files, raw)


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
    # ⚠ 2026-11-24：档位名（12:00/15:00/...），写进 manifest.completedSlots
    #   门控的自愈补跑靠它精确判断「该档跑过没」，不再用时间推导。
    ap.add_argument("--slot", default="", help="本次完成的档位名（如 12:00）")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return selftest()
    if not args.incoming or not args.store:
        ap.error("需要 --incoming 与 --store")
    res = publish(Path(args.incoming), Path(args.store), slot=args.slot)
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

