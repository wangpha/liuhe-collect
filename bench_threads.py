"""在 GitHub Actions 真机上测 OCR 线程数的真实表现。

本地模拟（限制核心数）≠ 真实云服务器，这个脚本只在 Actions 里跑。
"""

import concurrent.futures as cf
import json
import os
import platform
import sys
import tempfile
import time
from pathlib import Path

os.environ["QODE_OCR_DML"] = "0"   # CI 无 GPU，强制 CPU

# 仓库结构：cloud/core/*.pyc，所以把 cloud/ 加进 sys.path 才能 from core import ocr
_ROOT = Path(__file__).resolve().parent
for _p in (str(_ROOT / "cloud"), str(_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from core import ocr
except ImportError as _exc:  # 给出可诊断的报错，而不是裸 ImportError
    print(f"[错误] 无法导入 core.ocr: {_exc}")
    print(f"  sys.path 前 5 项: {sys.path[:5]}")
    print(f"  cloud/core 存在: {(Path(_ROOT) / 'cloud' / 'core').exists()}")
    raise SystemExit(1)


def machine_info() -> dict:
    info = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count_logical": os.cpu_count(),
        "python": sys.version.split()[0],
    }
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as fh:
            for line in fh:
                if "model name" in line:
                    info["cpu_model"] = line.split(":", 1)[1].strip()
                    break
    except Exception:
        pass
    try:
        with open("/proc/meminfo", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("MemTotal"):
                    info["mem_total_gb"] = round(int(line.split()[1]) / 1024 / 1024, 1)
                    break
    except Exception:
        pass
    try:
        import psutil
        info["cpu_affinity"] = len(psutil.Process().cpu_affinity())
    except Exception:
        pass
    return info


def run_one(img: Path) -> float:
    """识别一张图，返回耗时。"""
    tmp = Path(tempfile.mkdtemp())
    t0 = time.perf_counter()
    ocr.analyze_image(
        img, target_period=318, cache_dir=tmp, force=True, source="基准测试"
    )
    return time.perf_counter() - t0


def bench(imgs: list[Path], threads: int, repeat: int = 3) -> dict:
    batch = imgs * repeat
    # 预热（把引擎加载时间排除在外）
    run_one(batch[0])
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=threads) as ex:
        list(ex.map(run_one, batch))
    wall = time.time() - t0
    return {
        "threads": threads,
        "images": len(batch),
        "wall_sec": round(wall, 2),
        "sec_per_image": round(wall / len(batch), 3),
    }


def main() -> None:
    imgs = sorted(Path("bench_imgs").glob("img_*.png"))
    if not imgs:
        print("[错误] 没有找到测试图片 bench_imgs/img_*.png")
        sys.exit(1)

    info = machine_info()
    print("=" * 70)
    print("机器信息")
    print("=" * 70)
    for k, v in info.items():
        print(f"  {k:<20} {v}")

    print()
    print("=" * 70)
    print(f"OCR 线程数基准测试（{len(imgs)} 张图 × 3 轮）")
    print("=" * 70)
    print(f"  {'线程数':<8}{'总耗时':<12}{'秒/张':<12}{'相对4线程':<12}")

    results = []
    base = None
    for th in (1, 2, 4, 6, 8, 12, 16):
        try:
            r = bench(imgs, th)
            results.append(r)
            if th == 4:
                base = r["sec_per_image"]
            speedup = f"{base / r['sec_per_image']:.2f}x" if base else "-"
            print(
                f"  {th:<8}{r['wall_sec']:<12}{r['sec_per_image']:<12}{speedup:<12}"
            )
            sys.stdout.flush()
        except Exception as exc:  # noqa: BLE001
            print(f"  {th:<8}失败: {type(exc).__name__}: {str(exc)[:60]}")
            sys.stdout.flush()

    print()
    print("=" * 70)
    print("结论")
    print("=" * 70)
    if results:
        best = min(results, key=lambda r: r["sec_per_image"])
        print(f"  最快配置: {best['threads']} 线程 → {best['sec_per_image']} 秒/张")
        four = next((r for r in results if r["threads"] == 4), None)
        eight = next((r for r in results if r["threads"] == 8), None)
        if four and eight:
            delta = (four["sec_per_image"] - eight["sec_per_image"]) / four["sec_per_image"] * 100
            verdict = "更快" if delta > 3 else ("更慢" if delta < -3 else "基本一样")
            print(
                f"  8 线程 vs 4 线程: {delta:+.1f}% → 8 线程{verdict}"
            )
        Path("bench_result.json").write_text(
            json.dumps({"machine": info, "results": results}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print("  详细结果: bench_result.json")


if __name__ == "__main__":
    main()
