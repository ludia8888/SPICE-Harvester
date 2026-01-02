#!/usr/bin/env python3
"""Generate docs/BACKEND_METHODS.md from backend/**/*.py using the AST."""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

SCOPE_LINE = "backend/**/*.py (including scripts and tests, excluding __pycache__)"


@dataclass
class FunctionInfo:
    signature: str
    line: int
    doc: str


@dataclass
class ClassInfo:
    name: str
    line: int
    doc: str
    methods: List[FunctionInfo]


@dataclass
class FileInfo:
    group: str
    path: str
    functions: List[FunctionInfo]
    classes: List[ClassInfo]


def iter_python_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        yield path


def summarize_docstring(node: ast.AST) -> str:
    doc = ast.get_docstring(node)
    if not doc:
        return "no docstring"
    for line in doc.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return "no docstring"


def format_signature(node: ast.AST) -> str:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        raise TypeError("node must be FunctionDef or AsyncFunctionDef")
    prefix = "async " if isinstance(node, ast.AsyncFunctionDef) else ""
    parts: List[str] = []
    for arg in node.args.posonlyargs:
        parts.append(arg.arg)
    for arg in node.args.args:
        parts.append(arg.arg)
    if node.args.vararg:
        parts.append(f"*{node.args.vararg.arg}")
    for arg in node.args.kwonlyargs:
        parts.append(arg.arg)
    if node.args.kwarg:
        parts.append(f"**{node.args.kwarg.arg}")
    return f"{prefix}{node.name}({', '.join(parts)})"


def collect_file_info(root: Path, path: Path) -> FileInfo:
    rel_path = path.relative_to(root)
    group = rel_path.parts[0] if len(rel_path.parts) > 1 else rel_path.name
    display_path = f"backend/{rel_path.as_posix()}"

    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(rel_path))

    functions: List[FunctionInfo] = []
    classes: List[ClassInfo] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(
                FunctionInfo(
                    signature=format_signature(node),
                    line=node.lineno,
                    doc=summarize_docstring(node),
                )
            )
        elif isinstance(node, ast.ClassDef):
            methods: List[FunctionInfo] = []
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods.append(
                        FunctionInfo(
                            signature=format_signature(item),
                            line=item.lineno,
                            doc=summarize_docstring(item),
                        )
                    )
            classes.append(
                ClassInfo(
                    name=node.name,
                    line=node.lineno,
                    doc=summarize_docstring(node),
                    methods=methods,
                )
            )

    return FileInfo(
        group=group,
        path=display_path,
        functions=functions,
        classes=classes,
    )


def current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def read_existing_timestamp(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("> Generated:"):
            return line.split(":", 1)[1].strip()
    return None


def render_index(files: List[FileInfo], timestamp: str) -> str:
    lines: List[str] = []
    lines.append("# Backend Method Index")
    lines.append("")
    lines.append(f"> Generated: {timestamp}")
    lines.append(f"> Scope: {SCOPE_LINE}")
    lines.append("")

    files_sorted = sorted(files, key=lambda info: (info.group, info.path))
    group = None
    for info in files_sorted:
        if info.group != group:
            group = info.group
            lines.append(f"## {group}")
            lines.append("")

        lines.append(f"### `{info.path}`")
        if info.functions:
            lines.append("- **Functions**")
            for fn in info.functions:
                lines.append(f"  - `{fn.signature}` (line {fn.line}): {fn.doc}")
        if info.classes:
            lines.append("- **Classes**")
            for cls in info.classes:
                lines.append(f"  - `{cls.name}` (line {cls.line}): {cls.doc}")
                for method in cls.methods:
                    lines.append(
                        f"    - `{method.signature}` (line {method.line}): {method.doc}"
                    )
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "backend",
        help="Backend root to scan (default: repo_root/backend)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "docs" / "BACKEND_METHODS.md",
        help="Output markdown file path",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    backend_root: Path = args.root
    output_path: Path = args.output

    if not backend_root.exists():
        raise SystemExit(f"Backend root not found: {backend_root}")

    files: List[FileInfo] = [
        collect_file_info(backend_root, path) for path in iter_python_files(backend_root)
    ]

    timestamp = current_timestamp()
    if args.check:
        existing_timestamp = read_existing_timestamp(output_path)
        if existing_timestamp:
            timestamp = existing_timestamp

    content = render_index(files, timestamp)

    if args.check:
        existing = output_path.read_text(encoding="utf-8") if output_path.exists() else ""
        if existing != content:
            print(
                f"{output_path} is out of date. Run: python scripts/generate_backend_methods.py",
                flush=True,
            )
            return 1
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
