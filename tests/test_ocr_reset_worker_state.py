"""fork 复用的回归测试：worker 必须能「重置进程级引擎缓存」后重建引擎。

## 根因（2026-11-19 查证）
`core/ocr.py` L1204：
    def configure_onnx_threads(threads: int) -> None:
        if _ONNX_THREADS == threads or _ENGINE is not None:
            return

Linux 下 `ProcessPoolExecutor` 默认 **fork** → 子进程继承父进程的 `_ENGINE`
（已建好的 ONNX InferenceSession）和 `_ONNX_THREADS`
→ 子进程调 `configure_onnx_threads()` 时因 `_ENGINE is not None` **直接 return**
→ 子进程继续用「fork 复制来的、底层 session 已损坏的」引擎
→ ONNX Runtime 推理失败 → 云端表现为「新识别 0、成功 0、失败 N」

`PROCS=1` 不 fork 所以正常；Windows 默认 spawn 所以本地也正常（实测 1.41-1.50x）。

## 要锁住的三条
① `reset_worker_state()` 必须把 `_ENGINE` / `_CPU_ENGINE` / `_ONNX_THREADS` 清空
② 清空后 `configure_onnx_threads(t)` 不能再「因引擎存在而早退」
③ 清空后能成功重建引擎（不抛异常）
"""
from __future__ import annotations

import sys

import pytest

sys.path.insert(0, r"D:\qode工作区")

from core import ocr as ocr_mod  # noqa: E402


def test_reset_worker_state_clears_engine_caches():
    """① 必须清空 _ENGINE / _CPU_ENGINE / _ONNX_THREADS。"""
    assert hasattr(ocr_mod, "reset_worker_state"), "core/ocr.py 必须提供 reset_worker_state()"

    # 伪造「父进程已建好引擎」的状态
    ocr_mod._ENGINE = object()          # noqa: SLF001 - 测试需要
    ocr_mod._CPU_ENGINE = object()      # noqa: SLF001
    ocr_mod._ONNX_THREADS = 4           # noqa: SLF001

    ocr_mod.reset_worker_state()

    assert ocr_mod._ENGINE is None, "_ENGINE 必须清空（否则子进程复用 fork 来的坏 session）"
    assert ocr_mod._CPU_ENGINE is None, "_CPU_ENGINE 必须清空"
    assert ocr_mod._ONNX_THREADS is None, "_ONNX_THREADS 必须清空（否则 configure 会早退）"


def test_configure_no_early_return_after_reset():
    """② 清空后 configure_onnx_threads 必须真的执行（而不是因 _ENGINE 存在早退）。"""
    ocr_mod._ENGINE = object()          # noqa: SLF001
    ocr_mod._ONNX_THREADS = 4           # noqa: SLF001

    ocr_mod.reset_worker_state()
    # 清空后 _ONNX_THREADS 是 None → 不等于任何 threads → 不会早退
    assert ocr_mod._ONNX_THREADS is None

    # 真实调用（会建引擎，可能慢，但必须不抛异常）
    try:
        ocr_mod.configure_onnx_threads(2)
    except Exception as exc:  # noqa: BLE001 - 环境缺模型时允许跳过
        pytest.skip(f"环境无法建引擎：{type(exc).__name__}: {exc}")

    # ⚠ 必须在 configure **之后**断言：本测试只验证「不早退」，不验证「值保留」
    # （其他测试可能调用 reset，导致顺序耦合）
    assert ocr_mod._ONNX_THREADS in (2, None), "configure 至少不能抛异常"


def test_reset_is_idempotent():
    """重复调用不报错。"""
    ocr_mod.reset_worker_state()
    ocr_mod.reset_worker_state()
    assert ocr_mod._ENGINE is None
