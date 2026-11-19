"""云端运行环境垫片：让同一份 core 代码在 Linux / 无 GPU / 无 PyQt 的 runner 上跑。

**为什么需要**（2026-11-16 实测依赖闭包 20 模块 44,387 行）：
  - `core/ocr.py` 的 `_install_parent_watchdog()` 直接调 `ctypes.windll`，
    Linux 上 `ctypes.windll` 不存在 → AttributeError；
  - `core/ocr.py` / `core/scraper.py` 里的 DirectML 分支在 Linux 上必然失败，
    必须用官方开关 `QODE_OCR_DML=0` 落到 CPU；
  - runner 默认 ASCII 编码，中文日志会抛 UnicodeEncodeError。

这里只做「环境准备」，不改 core 源码（文档第九节第 5 条）。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

#: 云端只用 CPU（文档 3.6：GPU/CPU 识别结果逐字段一致；3.1：runner 无 GPU）
FORCE_ENV = {
    "QODE_OCR_DML": "0",          # 关掉 DirectML，走 rapidocr CPU
    # 进程数：**默认 1，但允许环境变量覆盖**（2026-11-20 变更）。
    #
    # ## 历史（2026-11-16 实测，run 43817560024）
    #   QODE_OCR_PROCS=4 → OCR 批次 **全失败**（110/281/91/124 张：新识别 0、成功 0、失败 N）
    #   QODE_OCR_PROCS=1 → 全部 0 失败
    # 所以当时硬编码 1 是「用串行换正确性」的取舍。
    #
    # ## 根因已查清并修复（2026-11-20）
    #   core/ocr.py L1204 `configure_onnx_threads()`：
    #       if _ONNX_THREADS == threads or _ENGINE is not None: return
    #   Linux 下 ProcessPoolExecutor 默认 **fork**，子进程继承父进程的 `_ENGINE`
    #   （ONNX InferenceSession 在 fork 后不可用）→ 早退 → 沿用坏引擎 → 推理全失败。
    #   已修：core/ocr.py 加 `reset_worker_state()`，`ocr_worker_initializer()` 在 fork 时调用。
    #   本地实测（Windows/spawn）：PROCS=1 → 3.07 秒/张，PROCS=2 → 2.19 秒/张（1.40x）。
    #
    # ## 现在的策略
    #   默认仍是 1（安全）；workflow 可设 `QODE_OCR_PROCS=2` 做云端 A/B，
    #   失败就删掉该环境变量即回到安全值。
    "QODE_OCR_PROCS": os.environ.get("QODE_OCR_PROCS") or "1",
    "PYTHONIOENCODING": "utf-8",
    "PYTHONUTF8": "1",
    "PYTHONUNBUFFERED": "1",
    # 关掉显卡租约：runner 没有显卡，留租约文件只会多一次文件锁
    "QODE_NO_GPU_LEASE": "1",
}


def _install_dummy_windll() -> None:
    """Linux 上给 ctypes 装一个假 windll。

    core/ocr.py::_install_parent_watchdog 会调
    `ctypes.windll.kernel32.OpenProcess(...)`，拿不到句柄时它自己就 `return`
    （源码注释：「打不开（父已死/无权限）就不监护，保持原行为」）。
    所以这里让 OpenProcess 返回 0，正是它预期的「不监护」分支，
    **不改变任何原有行为**，只是让 Linux 不抛 AttributeError。
    """
    import ctypes

    if hasattr(ctypes, "windll"):
        return

    class _DummyFunc:
        def __call__(self, *args, **kwargs):
            return 0

        def __getattr__(self, name):
            return _DummyFunc()

    class _DummyLib:
        def __getattr__(self, name):
            return _DummyFunc()

    class _DummyWindll:
        def __getattr__(self, name):
            return _DummyLib()

    ctypes.windll = _DummyWindll()  # type: ignore[attr-defined]


def _ensure_core_importable() -> None:
    """把 `core` 包放进 sys.path。

    优先级：
      1. 环境变量 LIUHE_CORE_PATH 指定的目录（本地开发指向 D:\\qode工作区）；
      2. 本文件同级的 `core/`（公开仓库里是 .pyc 二进制版）；
      3. 当前工作目录（本地从工作区根跑时）。
    """
    here = Path(__file__).resolve().parent
    override = os.environ.get("LIUHE_CORE_PATH", "").strip()
    candidates: list[Path] = []
    if override:
        candidates.append(Path(override))
    candidates.append(here)          # cloud/core/*.pyc
    candidates.append(Path.cwd())

    for cand in candidates:
        if (cand / "core").is_dir():
            p = str(cand)
            if p not in sys.path:
                sys.path.insert(0, p)
            os.environ.setdefault("LIUHE_CORE_ROOT", p)
            return
    raise RuntimeError(
        "找不到 core 包（已尝试: " + ", ".join(str(c) for c in candidates) + "）"
    )


def prepare(force_cpu: bool = True) -> dict:
    """在 import 任何 core 模块之前调用。返回生效的环境快照（写日志用）。"""
    if force_cpu:
        for key, value in FORCE_ENV.items():
            os.environ.setdefault(key, value)
    # 已经明确设过的不覆盖，但 DML 必须是关的，否则 Linux 上必炸
    if sys.platform != "win32":
        os.environ["QODE_OCR_DML"] = "0"

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - 管道被关掉时不允许影响主流程
            pass

    _install_dummy_windll()
    _ensure_core_importable()
    return {
        "platform": sys.platform,
        "python": sys.version.split()[0],
        "core_root": os.environ.get("LIUHE_CORE_ROOT", ""),
        "QODE_OCR_DML": os.environ.get("QODE_OCR_DML", ""),
        "QODE_OCR_PROCS": os.environ.get("QODE_OCR_PROCS", ""),
        "QODE_NO_GPU_LEASE": os.environ.get("QODE_NO_GPU_LEASE", ""),
    }
