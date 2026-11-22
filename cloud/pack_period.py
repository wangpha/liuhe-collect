"""单期产物打包/解包（解决 GitHub 100MB 单文件硬限制）。

## 为什么必须打成一个 tar.gz（2026-11-16 实测）
大港澳一期真实产物：
    992 帖 → users.json **209.18 MB**、posts.json **191.17 MB**
（本地基线 562 帖时 users.json 已是 90 MB —— 文档 3.5 表格里的 "16.35 MB" 是过时数据）
GitHub 单文件上限 **100 MB**，push 直接被 pre-receive hook 拒：

    remote: error: File .../users.json is 209.18 MB; exceeds GitHub's file size limit of 100.00 MB
    ! [remote rejected] main -> main (pre-receive hook declined)

所以数据仓改成「每期一个 tar.gz」：
  · 一个文件装下整期 21/22 个产物（解包后与本地目录逐字节一致）
  · 压缩比通常 5~10 倍，同时大幅减小仓库增长
  · 本地 sync 下载后校验 sha256 → 解包 → 得到与云端完全相同的产物目录

用法：
    python cloud/pack_period.py pack   --dir <期目录> --out <期.tar.gz>
    python cloud/pack_period.py unpack --archive <期.tar.gz> --dir <期目录>
    python cloud/pack_period.py selftest
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

#: 不打包进去的内容（图片体积大且按 4.2 不上仓库；.cache 是过程缓存）
EXCLUDE_DIRS = {"images", ".cache"}
EXCLUDE_SUFFIX = {".pyc"}


def _iter_files(root: Path):
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if path.suffix in EXCLUDE_SUFFIX:
            continue
        yield path, rel


def pack(period_dir: Path, archive: Path) -> dict:
    """把一期产物目录打成一个 .tar.gz，返回清单（文件数/sha256/大小）。"""
    period_dir = Path(period_dir)
    archive = Path(archive)
    if not period_dir.is_dir():
        raise SystemExit(f"目录不存在：{period_dir}")
    archive.parent.mkdir(parents=True, exist_ok=True)
    files = list(_iter_files(period_dir))
    if not files:
        raise SystemExit(f"目录里没有可打包的文件：{period_dir}")

    with tarfile.open(archive, "w:gz", compresslevel=6) as tar:
        for path, rel in files:
            info = tar.gettarinfo(str(path), arcname=str(rel))
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            with path.open("rb") as fh:
                tar.addfile(info, fh)

    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    return {
        "period": period_dir.name,
        "archive": str(archive),
        "files": len(files),
        "rawBytes": sum(p.stat().st_size for p, _ in files),
        "archiveBytes": archive.stat().st_size,
        "sha256": sha,
    }


def unpack(archive: Path, period_dir: Path, expected_sha256: str = "", expected_files: int = 0) -> dict:
    """把 .tar.gz 解到指定目录，并做 sha256 / 文件数校验。"""
    archive = Path(archive)
    period_dir = Path(period_dir)
    if not archive.is_file():
        raise SystemExit(f"归档不存在：{archive}")

    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    if expected_sha256 and sha != expected_sha256:
        raise SystemExit(f"sha256 不匹配：{archive}\n  期望 {expected_sha256}\n  实际 {sha}")

    if period_dir.exists():
        shutil.rmtree(period_dir)
    period_dir.mkdir(parents=True, exist_ok=True)

    # 防目录穿越：只允许相对路径
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            name = member.name.replace("\\", "/")
            if name.startswith("/") or ".." in name.split("/"):
                raise SystemExit(f"归档里含非法路径：{name}")
        # filter="data" 是 3.12+ 的推荐写法，避免 DeprecationWarning（云端零警告口径）
        try:
            tar.extractall(period_dir, filter="data")
        except TypeError:
            tar.extractall(period_dir)

    actual = sum(1 for p in period_dir.rglob("*") if p.is_file())

    # ⚠ 2026-11-22 根因修复（实测数据仓 320-325 期 manifest files=0）：
    #   原来 `passed = (expected_files == 0 or actual == expected_files)`，
    #   expected_files=0 时永远 True —— 空包、解压失败、缺核心产物都发现不了。
    #   现在加三层校验，expected_files=0 也能真校验。
    try:
        with tarfile.open(archive, "r:gz") as _t:
            _members = [m for m in _t.getmembers() if not m.isdir()]
        in_archive = len(_members)
        _names = {m.name.rsplit("/", 1)[-1] for m in _members}
    except (OSError, tarfile.TarError):
        in_archive = -1
        _names = set()

    # 核心产物：三源都必有的
    CORE = {"posts.json", "users.json"}
    missing_core = sorted(CORE - _names)

    # 校验三项
    ok_nonempty = actual > 0                     # ① 解压后非空
    ok_count = (in_archive == actual)            # ② 解压数与归档成员数一致
    ok_core = not missing_core                   # ③ 核心产物齐
    ok_expected = (expected_files == 0) or (actual == expected_files)   # ④ 期望值（有才比）

    passed = ok_nonempty and ok_count and ok_core and ok_expected

    return {
        "period": period_dir.name,
        "dir": str(period_dir),
        "files": actual,
        "inArchive": in_archive,
        "expectedFiles": expected_files,
        "missingCore": missing_core,
        "sha256": sha,
        "checks": {
            "非空": ok_nonempty,
            "成员数一致": ok_count,
            "核心产物齐": ok_core,
            "期望文件数": ok_expected,
        },
        "passed": passed,
    }


def selftest() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="packperiod-"))
    src = tmp / "澳门_2026_320期"
    (src / "images" / "u1").mkdir(parents=True)
    (src / ".cache" / "ocr").mkdir(parents=True)
    (src / "posts.json").write_text('{"a":1}', encoding="utf-8")
    (src / "users.json").write_text('{"b":2}', encoding="utf-8")
    (src / "images" / "u1" / "x.png").write_bytes(b"png")
    (src / ".cache" / "ocr" / "y.json").write_text("{}", encoding="utf-8")

    info = pack(src, tmp / "p.tar.gz")
    out = tmp / "extract" / "澳门_2026_320期"
    res = unpack(tmp / "p.tar.gz", out, info["sha256"], info["files"])

    ok = True

    def check(cond, msg):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + msg)
        ok = ok and cond

    print("自检：")
    check(info["files"] == 2, f"应只打包 2 个产物文件（排除 images/.cache），实际 {info['files']}")
    check(res["files"] == 2, f"解包应得 2 个文件，实际 {res['files']}")
    check((out / "posts.json").read_text(encoding="utf-8") == '{"a":1}', "posts.json 内容一致")
    check(not (out / "images").exists(), "images 未被解出")
    check(not (out / ".cache").exists(), ".cache 未被解出")
    try:
        unpack(tmp / "p.tar.gz", out, "deadbeef")
        check(False, "sha256 校验应失败")
    except SystemExit:
        check(True, "sha256 不匹配会被拒绝")
    shutil.rmtree(tmp, ignore_errors=True)
    print("结论：" + ("自检通过 ✅" if ok else "自检失败 ❌"))
    return 0 if ok else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="期产物打包/解包")
    sub = ap.add_subparsers(dest="cmd", required=False)
    p = sub.add_parser("pack")
    p.add_argument("--dir", required=True)
    p.add_argument("--out", required=True)
    u = sub.add_parser("unpack")
    u.add_argument("--archive", required=True)
    u.add_argument("--dir", required=True)
    u.add_argument("--sha256", default="")
    u.add_argument("--files", type=int, default=0)
    sub.add_parser("selftest")
    args = ap.parse_args(argv)

    if not args.cmd or args.cmd == "selftest":
        return selftest()
    if args.cmd == "pack":
        info = pack(Path(args.dir), Path(args.out))
        print(json.dumps(info, ensure_ascii=False))
        return 0
    res = unpack(Path(args.archive), Path(args.dir), args.sha256, args.files)
    print(json.dumps(res, ensure_ascii=False))
    return 0 if res["passed"] else 3


if __name__ == "__main__":
    raise SystemExit(main())

