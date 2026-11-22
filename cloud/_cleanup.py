"""R2 图片两天轮转 + R7 产物 30 期轮转（M1 / M3）。

规则（用户原话）：
  「图片是如今天的数据那么就是明天开机后清理今天的图片，避免重复识别」
  → 保留「当天 + 前一天」，更早的删除。

判定依据用**图片文件自身的日期**（mtime），不用目录名 —— 目录名会随跨天运行变化。
"""
from __future__ import annotations

import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"}


def _file_date(path: Path) -> date:
    return datetime.fromtimestamp(path.stat().st_mtime).date()


def cleanup_images(
    roots: list[Path],
    keep_days: int = 2,
    today: date | None = None,
    dry_run: bool = False,
    log: Callable[[str], None] | None = None,
) -> dict:
    """删除「今天」和「昨天」之外的图片。

    keep_days=2 表示保留 today 与 today-1，删除 today-2 及更早。
    """
    say = log or (lambda message: None)
    today = today or date.today()
    oldest_keep = today - timedelta(days=keep_days - 1)   # 含当天共 keep_days 天
    deleted = kept = 0
    freed = 0
    per_day: dict[str, int] = {}

    for root in roots:
        if not root or not Path(root).is_dir():
            continue
        for path in Path(root).rglob("*"):
            if not path.is_file() or path.suffix.lower() not in IMAGE_SUFFIXES:
                continue
            try:
                day = _file_date(path)
            except OSError:
                continue
            per_day[str(day)] = per_day.get(str(day), 0) + 1
            if day >= oldest_keep:
                kept += 1
                continue
            if not dry_run:
                try:
                    freed += path.stat().st_size
                    path.unlink()
                except OSError as exc:
                    say(f"[清理图片] 删除失败 {path}：{exc}")
                    continue
            deleted += 1

    say(
        f"[清理图片] 保留 {kept} 张（{oldest_keep} 及以后），删除 {deleted} 张"
        f"{'（dry-run 未实际删除）' if dry_run else f'，释放 {freed/1024/1024:.1f} MB'}"
    )
    if per_day:
        say("[清理图片] 各日期图片数：" + ", ".join(f"{k}={v}" for k, v in sorted(per_day.items())))
    return {
        "keepDays": keep_days,
        "oldestKeep": str(oldest_keep),
        "today": str(today),
        "kept": kept,
        "deleted": deleted,
        "freedBytes": freed,
        "byDate": per_day,
        "dryRun": dry_run,
    }


def _period_sort_key(path: Path | str) -> tuple[int, str]:
    """期目录名形如 `澳门_2026_317期`，按其中的期号排序。

    2026-11-23：接受 Path 或 str（轮转时两种形态都可能传进来，
    目录用 Path、`.tar.gz` 去掉后缀后用 str）。
    """
    name = path.name if isinstance(path, Path) else str(path)
    digits = "".join(ch for ch in name.split("_")[-1] if ch.isdigit())
    try:
        return (int(digits or 0), name)
    except ValueError:
        return (0, name)


#: 期归档的后缀（与 cloud/publish_store.py 的 ARCHIVE_SUFFIX 一致）
_ARCHIVE_SUFFIX = ".tar.gz"


def _collect_period_entries(source_dir: Path) -> dict[str, list[Path]]:
    """扫一个源目录，返回 {期名: [该期的所有路径]}。

    ## 为什么需要（2026-11-23 修复 R7「30 天轮转从未生效」）
    原来只认**子目录**（`p.is_dir()`），但 publish_store 写的是
    `data/<源>/澳门_2026_XXX期.tar.gz`（**文件**）→ 期列表恒为空 → 永不轮转。
    实测数据仓三源各有 8 期，日志却写「共 0 期」。

    现在两种形态都识别：
      · 目录：`澳门_2026_XXX期/`            → 期名 = 目录名
      · 文件：`澳门_2026_XXX期.tar.gz`      → 期名 = 去掉 .tar.gz
    非期文件（如 .gitkeep）忽略。
    """
    entries: dict[str, list[Path]] = {}
    try:
        children = list(source_dir.iterdir())
    except OSError:
        return entries
    for child in children:
        name = child.name
        try:
            if child.is_dir():
                entries.setdefault(name, []).append(child)
            elif child.is_file() and name.endswith(_ARCHIVE_SUFFIX):
                period = name[: -len(_ARCHIVE_SUFFIX)]
                entries.setdefault(period, []).append(child)
        except OSError:
            continue
    return entries


def rotate_periods(
    store_root: Path,
    keep: int = 30,
    dry_run: bool = False,
    log: Callable[[str], None] | None = None,
) -> dict:
    """数据仓里每个源只保留最近 `keep` 期，更早的整期删除（R7）。

    支持两种形态：期目录（历史）与 `.tar.gz` 归档（当前）。
    """
    say = log or (lambda message: None)
    store_root = Path(store_root)
    removed: list[str] = []
    kept_per_source: dict[str, list[str]] = {}

    if not store_root.is_dir():
        say(f"[轮转] 数据仓目录不存在，跳过：{store_root}")
        return {"sources": {}, "removed": [], "keep": keep}

    for source_dir in sorted(p for p in store_root.iterdir() if p.is_dir()):
        entries = _collect_period_entries(source_dir)
        period_names = sorted(entries, key=_period_sort_key)
        keep_list = period_names[-keep:] if len(period_names) > keep else period_names
        drop_list = period_names[: max(0, len(period_names) - keep)]
        kept_per_source[source_dir.name] = keep_list
        for old_name in drop_list:
            paths = entries.get(old_name) or []
            shapes = []
            for old in paths:
                removed.append(f"{source_dir.name}/{old.name}")
                try:
                    is_dir = old.is_dir()
                except OSError:
                    continue
                shapes.append("目录" if is_dir else "文件")
                if dry_run:
                    continue
                if is_dir:
                    shutil.rmtree(old, ignore_errors=True)
                else:
                    try:
                        old.unlink()
                    except OSError:
                        pass
            shapes_txt = "/".join(sorted(set(shapes))) if shapes else "?"
            say(f"[轮转]   删除 {source_dir.name}/{old_name}（{shapes_txt}）")
        say(
            f"[轮转] {source_dir.name}: 共 {len(period_names)} 期，保留 {len(keep_list)} 期"
            + (f"，删除 {len(drop_list)} 期：{drop_list}" if drop_list else "")
            + ("（dry-run 未实际删除）" if dry_run and drop_list else "")
        )
    if not removed:
        say(f"[轮转] 没有超过 {keep} 期的源，无需删除")
    return {
        "keep": keep,
        "keepList": kept_per_source,
        "removed": removed,
        "removedCount": len(removed),
        "dryRun": dry_run,
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
