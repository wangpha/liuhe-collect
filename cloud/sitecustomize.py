"""云端进程级静音：把 onnxruntime 的日志级别降到 ERROR。

## 为什么需要（2026-11-16 Gate1 实测）
云端 runner 是无显卡的 Linux，onnxruntime 建会话时会探测 GPU：

    [W:onnxruntime:Default, device_discovery.cc:325 DiscoverDevicesForPlatform]
    GPU device discovery failed: ... Failed to open file: "/sys/class/drm/card0/device/vendor"

这行是 **WARNING**，而文档 6B.3 规定「出现任何 WARNING 即不通过」。
它不是故障：我们在云端强制 `QODE_OCR_DML=0`（纯 CPU），本来就不该探测 GPU；
同一轮 OCR 统计是「失败 0 / 缓存命中 100%」。

## 为什么用 sitecustomize 而不是改 core
`core/ocr.py` 一行不改（文档第九节第 5 条）。sitecustomize 是 Python 启动时
自动导入的钩子：只要它在 sys.path 上，**父进程、`python -m core.scraper` 子进程、
以及它 spawn 出来的 OCR worker 都会先执行这里**，一处生效、覆盖全链路。

## 这不是"让校验看不见问题"
级别只降到 ERROR（3），**真错误照样打印**（`[E:onnxruntime:...]` 依然出现）。
被压掉的只有 verbose/info/warning 这类诊断噪音。仅当 `QODE_OCR_DML=0`（云端 CPU 模式）
时才生效，本地 Windows GPU 运行完全不受影响。
"""
from __future__ import annotations

import os


def _quiet_onnxruntime() -> None:
    if os.environ.get("QODE_OCR_DML", "") != "0":
        return
    if os.environ.get("LIUHE_ORT_QUIET", "1") != "1":
        return
    try:
        import onnxruntime as ort

        # 0=verbose 1=info 2=warning 3=error 4=fatal
        ort.set_default_logger_severity(3)
    except Exception:  # noqa: BLE001 - 装不上/导入失败都不该影响任何业务流程
        pass


_quiet_onnxruntime()
