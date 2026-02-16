#!/usr/bin/env python3
"""Generate portal config reference from backend/shared/config/settings.py.

Parses Pydantic BaseSettings subclasses via AST to extract:
- Class names and docstrings
- Field names, types, defaults, descriptions
- Environment variable bindings (validation_alias / AliasChoices)

Produces:
- docs-portal/docs/reference/auto-config-reference.mdx  (human-readable MDX)
- docs-portal/static/generated/config-landscape.json      (visualization data)
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
SETTINGS_PATH = REPO_ROOT / "backend" / "shared" / "config" / "settings.py"
PORTAL_DOCS = REPO_ROOT / "docs-portal" / "docs"
PORTAL_GENERATED = REPO_ROOT / "docs-portal" / "static" / "generated"


@dataclass
class FieldInfo:
    name: str
    type_annotation: str
    default: Optional[str]
    description: str
    env_vars: List[str] = field(default_factory=list)
    required: bool = False


@dataclass
class SettingsClass:
    name: str
    docstring: str
    fields: List[FieldInfo] = field(default_factory=list)
    base_class: str = "BaseSettings"


# ---------------------------------------------------------------------------
# AST parsing
# ---------------------------------------------------------------------------

def _extract_string_value(node: ast.expr) -> Optional[str]:
    """Extract string value from AST node."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_field_kwargs(call: ast.Call) -> Dict[str, Any]:
    """Extract keyword arguments from a Field(...) call."""
    kwargs: Dict[str, Any] = {}
    for kw in call.keywords:
        if kw.arg is None:
            continue
        if isinstance(kw.value, ast.Constant):
            kwargs[kw.arg] = kw.value.value
        elif isinstance(kw.value, ast.Call):
            # Handle AliasChoices("FOO", "BAR")
            if isinstance(kw.value.func, ast.Name) and kw.value.func.id == "AliasChoices":
                aliases = []
                for arg in kw.value.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        aliases.append(arg.value)
                kwargs[kw.arg] = aliases
            elif isinstance(kw.value.func, ast.Name) and kw.value.func.id == "lambda":
                kwargs[kw.arg] = "<dynamic>"
        elif isinstance(kw.value, ast.Lambda):
            # Try to extract lambda body
            body = kw.value.body
            if isinstance(body, ast.IfExp):
                # lambda: ("x" if cond else "y")
                kwargs[kw.arg] = "<env-dependent>"
            elif isinstance(body, ast.Constant):
                kwargs[kw.arg] = body.value
        elif isinstance(kw.value, ast.Name):
            if kw.value.id in ("True", "False", "None"):
                kwargs[kw.arg] = {"True": True, "False": False, "None": None}[kw.value.id]
            else:
                kwargs[kw.arg] = kw.value.id
    return kwargs


def _extract_type_annotation(node: Optional[ast.expr]) -> str:
    """Convert AST type annotation to string."""
    if node is None:
        return "Any"
    return ast.unparse(node)


def _parse_settings_classes(source: str) -> List[SettingsClass]:
    """Parse settings.py to extract all BaseSettings subclasses."""
    tree = ast.parse(source)
    classes: List[SettingsClass] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue

        # Check if it inherits from BaseSettings
        base_names = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                base_names.append(base.id)
            elif isinstance(base, ast.Attribute):
                base_names.append(ast.unparse(base))

        if "BaseSettings" not in base_names:
            continue

        docstring = ast.get_docstring(node) or ""
        cls = SettingsClass(
            name=node.name,
            docstring=docstring.strip(),
            base_class="BaseSettings",
        )

        for item in node.body:
            if not isinstance(item, ast.AnnAssign):
                continue
            if not isinstance(item.target, ast.Name):
                continue

            field_name = item.target.id
            type_ann = _extract_type_annotation(item.annotation)

            # Skip model_config
            if field_name == "model_config":
                continue

            default_val = None
            description = ""
            env_vars: List[str] = []
            required = True

            if item.value is not None:
                if isinstance(item.value, ast.Call):
                    func = item.value.func
                    func_name = ""
                    if isinstance(func, ast.Name):
                        func_name = func.id
                    elif isinstance(func, ast.Attribute):
                        func_name = func.attr

                    if func_name == "Field":
                        kwargs = _extract_field_kwargs(item.value)
                        description = kwargs.get("description", "")
                        default_val = kwargs.get("default")
                        if "default_factory" in kwargs:
                            default_val = kwargs["default_factory"]
                            required = False
                        if default_val is not None:
                            required = False

                        # Extract env vars from validation_alias
                        alias = kwargs.get("validation_alias")
                        if isinstance(alias, list):
                            env_vars = alias
                        elif isinstance(alias, str):
                            env_vars = [alias]

                        # Also infer env var from field name (Pydantic convention)
                        upper_name = field_name.upper()
                        if upper_name not in env_vars:
                            env_vars.insert(0, upper_name)
                    else:
                        default_val = ast.unparse(item.value)
                        required = False
                elif isinstance(item.value, ast.Constant):
                    default_val = repr(item.value.value)
                    required = False

            cls.fields.append(FieldInfo(
                name=field_name,
                type_annotation=type_ann,
                default=str(default_val) if default_val is not None else None,
                description=str(description),
                env_vars=env_vars,
                required=required,
            ))

        if cls.fields:
            classes.append(cls)

    return classes


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def _render_config_mdx(classes: List[SettingsClass]) -> str:
    lines: List[str] = []
    lines.append("---")
    lines.append("title: Configuration Reference (Auto-Generated)")
    lines.append("sidebar_label: Configuration")
    lines.append("sidebar_position: 2")
    lines.append("---")
    lines.append("")
    lines.append("# Configuration Reference")
    lines.append("")
    lines.append(":::info Auto-Generated")
    lines.append("This page is auto-generated from `backend/shared/config/settings.py`.")
    lines.append("Do not edit manually. Run `python scripts/generate_portal_config_reference.py`.")
    lines.append(":::")
    lines.append("")

    total_fields = sum(len(c.fields) for c in classes)
    lines.append(f"SPICE Harvester uses **{len(classes)} configuration classes** with **{total_fields} settings** total.")
    lines.append("All settings use Pydantic BaseSettings with environment variable binding.")
    lines.append("")

    # TOC
    lines.append("## Configuration Classes")
    lines.append("")
    for cls in classes:
        # Docusaurus generates anchors by lowercasing the heading text
        anchor = cls.name.lower()
        lines.append(f"- [{cls.name}](#{anchor}) — {cls.docstring.split(chr(10))[0] if cls.docstring else f'{len(cls.fields)} settings'}")
    lines.append("")

    # Per-class sections
    for cls in classes:
        lines.append(f"## {cls.name}")
        lines.append("")
        if cls.docstring:
            first_line = cls.docstring.split("\n")[0]
            lines.append(f"> {first_line}")
            lines.append("")

        lines.append(f"**{len(cls.fields)} settings**")
        lines.append("")

        lines.append("| Setting | Type | Default | Env Variable | Description |")
        lines.append("|---------|------|---------|-------------|-------------|")

        for f in cls.fields:
            env_str = ", ".join(f"`{e}`" for e in f.env_vars[:2]) if f.env_vars else "-"
            default_str = f"`{f.default}`" if f.default is not None else "**required**" if f.required else "-"
            # Clean up long defaults
            if default_str and len(default_str) > 40:
                default_str = default_str[:37] + "...`"
            desc = f.description.replace("|", "\\|") if f.description else "-"
            type_str = f.type_annotation.replace("|", "\\|")
            lines.append(
                f"| `{f.name}` | `{type_str}` | {default_str} | {env_str} | {desc} |"
            )

        lines.append("")

    return "\n".join(lines)


def _render_config_json(classes: List[SettingsClass]) -> str:
    data = {
        "classes": [],
        "total_fields": sum(len(c.fields) for c in classes),
    }

    for cls in classes:
        cls_data = {
            "name": cls.name,
            "docstring": cls.docstring,
            "field_count": len(cls.fields),
            "fields": [],
        }
        for f in cls.fields:
            cls_data["fields"].append({
                "name": f.name,
                "type": f.type_annotation,
                "default": f.default,
                "description": f.description,
                "env_vars": f.env_vars,
                "required": f.required,
            })
        data["classes"].append(cls_data)

    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def generate(check: bool = False) -> int:
    if not SETTINGS_PATH.exists():
        print(f"WARN: {SETTINGS_PATH} not found. Skipping config reference.", file=sys.stderr)
        return 0

    source = SETTINGS_PATH.read_text(encoding="utf-8")
    classes = _parse_settings_classes(source)

    if not classes:
        print("WARN: No BaseSettings subclasses found.", file=sys.stderr)
        return 0

    outputs: Dict[Path, str] = {}

    # MDX reference
    mdx_path = PORTAL_DOCS / "reference" / "auto-config-reference.mdx"
    outputs[mdx_path] = _render_config_mdx(classes)

    # JSON for visualization
    json_path = PORTAL_GENERATED / "config-landscape.json"
    outputs[json_path] = _render_config_json(classes)

    failures: List[str] = []
    for path, content in outputs.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        if check:
            existing = path.read_text(encoding="utf-8") if path.exists() else ""
            if existing != content:
                failures.append(str(path))
                print(f"  OUT OF DATE: {path}")
        else:
            path.write_text(content, encoding="utf-8")
            print(f"  WROTE: {path}")

    if check and failures:
        print(
            f"\n{len(failures)} config reference file(s) out of date. "
            f"Run: python scripts/generate_portal_config_reference.py"
        )
        return 1

    if not check:
        print(f"\nGenerated config reference: {len(classes)} classes, {sum(len(c.fields) for c in classes)} fields")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if any output would change",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return generate(check=args.check)


if __name__ == "__main__":
    raise SystemExit(main())
