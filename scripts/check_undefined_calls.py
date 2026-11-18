import ast, sys
from pathlib import Path

problems = []
for f in sorted(Path(r"D:\qode工作区\cloud").glob("*.py")):
    tree = ast.parse(f.read_text(encoding="utf-8"))
    defined = set(dir(__builtins__)) | {"__file__", "__name__", "__doc__"}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    defined.add(t.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            defined.add(node.target.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for a in node.names:
                defined.add((a.asname or a.name).split(".")[0])
        elif isinstance(node, ast.arg):
            defined.add(node.arg)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            defined.add(node.name)
        elif isinstance(node, (ast.comprehension,)):
            pass
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            nm = node.func.id
            if nm not in defined and not nm.startswith("_builtin"):
                problems.append(f"{f.name}:{node.lineno}: 调用了未定义的名字 {nm}")
print(f"检查 {len(list(Path(r'D:/qode工作区/cloud').glob('*.py')))} 个模块")
if problems:
    for x in problems[:20]:
        print("  ⚠", x)
    sys.exit(1)
print("  ✅ 没有「调用未定义名字」的问题")
