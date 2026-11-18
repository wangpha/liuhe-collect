"""SIGTERM 优雅投降的测试（2026-11-19 用户裁决方案）。

## 背景
分片跑到 25 分钟被 GitHub job timeout **SIGKILL** → 没写 unfinished、没建产物、判 failure。
改成 `timeout --signal=SIGTERM 1440`（24 分钟）→ 可捕获 → 写 unfinished → `SystemExit(0)`
→ `_run_module_main` 捕获后 code=0 → do_shard 正常收尾 → **分片判 success**。

## 测试注意
发 SIGTERM 会触发 handler 抛 SystemExit —— 必须**捕获并恢复原 handler**，
否则 pytest 进程会被中断（第一版就踩了这个坑）。
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, r"D:\qode工作区")

from cloud import runner  # noqa: E402


def _surrender(work: Path, source: str, shard: int, planned: set[int], logs: list[str]):
    """装 handler → **直接调用**它 → 返回 SystemExit 的 code；无论结果都恢复原 handler。

    ## 为什么不用 os.kill(getpid(), SIGTERM)（第一版踩的坑）
    Windows 上 `os.kill(pid, SIGTERM)` 走 `TerminateProcess`，**不会调用 Python 的 handler**
    → pytest 进程被硬杀（退出码 15）。POSIX 才会投递给 Python 级 handler。
    而云端是 Linux（`timeout --signal=SIGTERM` 真实投递），所以直接调用 handler 等价且跨平台。
    """
    old = signal.getsignal(signal.SIGTERM)
    try:
        assert runner._install_sigterm_surrender(work, source, shard, planned, logs.append)
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler), "handler 必须已注册"
        with pytest.raises(SystemExit) as ei:
            handler(signal.SIGTERM, None)
        return ei.value.code
    finally:
        signal.signal(signal.SIGTERM, old if old is not None else signal.SIG_DFL)


def test_install_sigterm_surrender_returns_true(tmp_path: Path):
    """能装上 handler（返回 True）。"""
    old = signal.getsignal(signal.SIGTERM)
    try:
        ok = runner._install_sigterm_surrender(tmp_path, "tuku", 2, {101, 102, 103}, lambda m: None)
        assert ok is True
    finally:
        signal.signal(signal.SIGTERM, old if old is not None else signal.SIG_DFL)


def test_sigterm_writes_unfinished_and_exits_zero(tmp_path: Path):
    """收到 SIGTERM → 写 unfinished + SystemExit(0)。"""
    logs: list[str] = []
    planned = {42986769, 42986213, 42985826}
    code = _surrender(tmp_path, "tuku", 2, planned, logs)

    assert code == 0, "必须 exit 0（否则分片判 failure）"
    f = tmp_path / "unfinished" / "unfinished_tuku_2.json"
    assert f.exists(), "必须写 unfinished 文件"
    data = json.loads(f.read_text(encoding="utf-8"))
    assert sorted(data["ids"]) == sorted(planned)
    assert "SIGTERM" in data["reason"]
    assert any("SIGTERM" in m for m in logs), "必须打日志说明收到 SIGTERM"
    assert any("unfinished 已写" in m for m in logs), "必须打日志说明已写"


def test_sigterm_handler_is_idempotent(tmp_path: Path):
    """重复装 handler 不报错；以最后一次 planned_ids 为准。"""
    old = signal.getsignal(signal.SIGTERM)
    try:
        assert runner._install_sigterm_surrender(tmp_path, "aocai", 0, {1}, lambda m: None)
        assert runner._install_sigterm_surrender(tmp_path, "aocai", 0, {1, 2}, lambda m: None)
        handler = signal.getsignal(signal.SIGTERM)
        with pytest.raises(SystemExit) as ei:
            handler(signal.SIGTERM, None)
        assert ei.value.code == 0
    finally:
        signal.signal(signal.SIGTERM, old if old is not None else signal.SIG_DFL)

    data = json.loads(
        (tmp_path / "unfinished" / "unfinished_aocai_0.json").read_text(encoding="utf-8")
    )
    assert sorted(data["ids"]) == [1, 2], "应以最后一次的 planned_ids 为准"


def test_sigterm_reason_and_shape(tmp_path: Path):
    """unfinished 的结构必须与「预算裁剪」写的格式一致（`_collect_unfinished` 要能读）。"""
    _surrender(tmp_path, "dahkg", 11, {7, 8}, [])
    data = json.loads(
        (tmp_path / "unfinished" / "unfinished_dahkg_11.json").read_text(encoding="utf-8")
    )
    for key in ("source", "shard", "ids", "reason", "ts"):
        assert key in data, f"缺字段 {key}（_collect_unfinished 依赖它）"
    assert data["source"] == "dahkg"
    assert data["shard"] == 11
    assert isinstance(data["ids"], list)


def test_handler_installed_before_module_run():
    """do_shard 必须在跑模块**之前**装 handler（否则模块跑到一半才装，早期 SIGTERM 抓不到）。"""
    import inspect

    src = inspect.getsource(runner.do_shard)
    i_install = src.find("_install_sigterm_surrender(")
    i_argv = src.find('argv = ["--out"')
    i_run = src.find("_run_module_main")
    assert i_install > 0, "do_shard 里必须调用 _install_sigterm_surrender"
    assert 0 < i_install < i_argv < i_run, "装 handler 必须在 argv 构造和跑模块之前"
