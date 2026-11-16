"""六合采集器 · 云端采集包（GitHub Actions 侧）。

运行前请先 `import cloud._compat; cloud._compat.prepare()` —— runner.main 已自动处理。
"""

__all__ = ["runner", "_compat", "_period", "_reconcile", "_guard", "_cleanup"]
