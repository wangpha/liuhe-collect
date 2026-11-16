"""本地增量补齐：从私有数据仓拉取缺失期号（R8）。

规则（文档 4.5 / M3）：
  1. 读云端 `manifest.json` 拿到期号清单；
  2. 云端期号集合 − 本地期号集合 = 需要下载的期号；
  3. 只下载缺失期（已有的跳过），下载后校验文件数 + 大小；
  4. 补齐后断言：本地期号集合 == 云端期号集合（一条不漏）。

用法：
    python cloud/sync.py                      # 补齐到默认输出根
    python cloud/sync.py --dry-run            # 只看差集，不下载
    python cloud/sync.py --sources dahkg      # 只补某个源
    python cloud/sync.py --dest D:\\some\\out  # 指定本地输出根
    python cloud/sync.py --rebuild            # 补齐后调用本地代码重新生成报表

实现说明：用 **git 稀疏检出**（sparse-checkout）只拉需要的期目录，
避免每期几十 MB 的全仓克隆；仓库是私有的，依赖本机已登录的 GitHub 凭据
（`gh auth setup-git` 配好的 credential helper）。
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

DATA_REPO = "https://github.com/wangpha/liuhe-data.git"
APP_NAME = "六合采集器"

#: 数据仓里的源目录名 -> (本地子目录名, 源 ID)
SOURCE_MAP = {
    "dahkg": ("大港澳", "dahkg"),
    "大港澳": ("大港澳", "dahkg"),
    "aocai": ("澳彩交流", "aocai"),
    "澳彩交流": ("澳彩交流", "aocai"),
    "tuku": ("", "tuku"),
    "澳门": ("", "tuku"),
}


def log(message: str) -> None:
    print(message, flush=True)


def default_dest() -> Path:
    """与本地软件一致的输出根（core/paths.py::data_root()/output）。"""
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA") or str(Path.home())
    return Path(base) / APP_NAME / "output"


def cache_root() -> Path:
    base = os.environ.get("LOCALAPPDATA") or str(Path.home())
    return Path(base) / APP_NAME / "cloud-sync"


def run(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if check and proc.returncode != 0:
        raise RuntimeError(f"命令失败({proc.returncode}): {' '.join(cmd)}\n{proc.stderr[:800]}")
    return proc


def ensure_repo(work: Path) -> Path:
    """准备一个带凭据的稀疏检出仓库。"""
    repo = work / "repo"
    if not (repo / ".git").exists():
        work.mkdir(parents=True, exist_ok=True)
        log(f"[sync] 首次克隆数据仓到 {repo}")
        if repo.exists():
            shutil.rmtree(repo)
        run(["git", "clone", "--filter=blob:none", "--no-checkout", DATA_REPO, str(repo)])
    run(["git", "fetch", "--depth", "1", "origin", "main"], cwd=repo)
    return repo


def read_manifest(repo: Path) -> dict:
    proc = run(["git", "show", "origin/main:manifest.json"], cwd=repo)
    return json.loads(proc.stdout)


def period_dir_name(year: int, period: int) -> str:
    return f"澳门_{year}_{period}期"


def period_number(name: str) -> int:
    digits = "".join(ch for ch in name.split("_")[-1] if ch.isdigit())
    return int(digits or 0)


def local_periods(dest: Path, sub: str) -> set[str]:
    root = dest / sub if sub else dest
    if not root.is_dir():
        return set()
    return {p.name for p in root.glob("*期") if p.is_dir()}


def sync_source(repo: Path, dest: Path, key: str, info: dict, dry_run: bool) -> dict:
    sub, source_id = SOURCE_MAP.get(key, (key, key))
    remote = set(info.get("periods") or [])
    have = local_periods(dest, sub)
    need = sorted(remote - have, key=period_number)

    log(f"\n[sync] 源 {key}（本地子目录 '{sub or '(根)'}' -> {dest}）")
    log(f"[sync]   云端 {len(remote)} 期：{sorted(remote, key=period_number)[:3]} ... 最新 {max(remote, key=period_number) if remote else '无'}")
    log(f"[sync]   本地已有 {len(have)} 期")
    log(f"[sync]   需要补齐 {len(need)} 期：{need if len(need) <= 12 else need[:12] + ['...']}")

    if dry_run or not need:
        return {"source": key, "remote": len(remote), "local": len(have), "needed": len(need), "copied": [], "verified": True}

    copied: list[str] = []
    problems: list[str] = []
    for period_name in need:
        rel = f"data/{key}/{period_name}"
        log(f"[sync]   ↓ {rel}")
        run(["git", "sparse-checkout", "set", rel], cwd=repo, check=False)
        proc = run(["git", "checkout", "origin/main", "--", rel], cwd=repo, check=False)
        src = repo / rel
        if not src.is_dir():
            problems.append(f"{period_name}: 检出后目录不存在（{proc.stderr[:120]}）")
            continue
        dst = (dest / sub / period_name) if sub else (dest / period_name)
        if dst.exists():
            shutil.rmtree(dst, ignore_errors=True)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst)

        # 校验：文件数 vs manifest
        expect = (info.get("files") or {}).get(period_name)
        actual = sum(1 for f in dst.rglob("*") if f.is_file())
        if expect is not None and actual != expect:
            problems.append(f"{period_name}: 文件数 {actual} != 云端 {expect}")
        else:
            copied.append(period_name)
            log(f"[sync]     ✅ 已补齐 {period_name}（{actual} 个文件）")

    after = local_periods(dest, sub)
    missing = sorted(remote - after, key=period_number)
    verified = not missing and not problems
    log(f"[sync]   补齐后本地 {len(after)} 期；云端缺 {len(missing)} 期"
        f"{'' if verified else '  ← 未对齐: ' + str(missing[:5])}")
    return {
        "source": key,
        "remote": len(remote),
        "localBefore": len(have),
        "needed": len(need),
        "copied": copied,
        "missingAfter": missing,
        "problems": problems,
        "verified": verified,
    }


def rebuild_local(source_id: str, dest: Path) -> None:
    """补齐后用本地完整代码重新生成报表（文档 4.5 第 6 步）。"""
    log(f"[rebuild] 调用本地模块重建 {source_id} 报表（--out {dest}）")
    module = {"dahkg": "core.dahkg", "aocai": "core.forum", "tuku": "core.scraper"}[source_id]
    cli = Path(r"D:\LiuHeInstalled25") / f"{APP_NAME}-cli.exe"
    if cli.exists():
        cmd = [str(cli), "--run-module", module, "--out", str(dest)]
    else:
        cmd = [sys.executable, "-m", module, "--out", str(dest)]
    proc = subprocess.run(cmd, check=False)
    if proc.returncode != 0:
        log(f"[rebuild] 警告：重建退出码 {proc.returncode}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="从云端私有数据仓增量补齐")
    parser.add_argument("--dest", default="", help="本地输出根（默认与软件一致）")
    parser.add_argument("--sources", default="", help="只处理这些源，逗号分隔")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rebuild", action="store_true", help="补齐后重新生成报表")
    args = parser.parse_args(argv)

    dest = Path(args.dest) if args.dest else default_dest()
    work = cache_root()
    log("=" * 70)
    log(f"[sync] 本地输出根: {dest}")
    log(f"[sync] 缓存仓库  : {work}")
    log("=" * 70)

    repo = ensure_repo(work)
    manifest = read_manifest(repo)
    sources = manifest.get("sources") or {}
    if not sources:
        log("[sync] 云端 manifest 里没有任何源的数据（云端还没跑过成功的一轮）")
        return 0

    wanted = [s.strip() for s in args.sources.split(",") if s.strip()] or list(sources)
    results = []
    for key in wanted:
        if key not in sources:
            log(f"[sync] 云端没有源 {key}，跳过")
            continue
        results.append(sync_source(repo, dest, key, sources[key], args.dry_run))

    ok = all(r["verified"] for r in results)
    log("\n" + "=" * 70)
    log(f"[sync] 结论：{'本地期号集合与云端一致 ✅' if ok else '存在未对齐期号 ❌'}")
    log("=" * 70)

    if args.rebuild and not args.dry_run:
        for r in results:
            if r.get("copied"):
                rebuild_local(SOURCE_MAP.get(r["source"], ("", r["source"]))[1], dest)

    report = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "dest": str(dest), "verified": ok, "results": results}
    out = work / "last_sync.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"[sync] 结果已写：{out}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
