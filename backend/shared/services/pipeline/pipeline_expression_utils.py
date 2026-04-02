from __future__ import annotations


def normalize_preview_comparison_expression(expression: str) -> str:
    """Translate standalone Spark-style '=' comparisons to Python '=='.

    The rewrite is quote-aware so string literals such as ``"a=b"`` remain unchanged.
    """
    text = str(expression or "")
    if not text:
        return text

    out: list[str] = []
    in_single = False
    in_double = False
    escaped = False
    length = len(text)

    for index, char in enumerate(text):
        if escaped:
            out.append(char)
            escaped = False
            continue

        if char == "\\":
            out.append(char)
            escaped = True
            continue

        if char == "'" and not in_double:
            in_single = not in_single
            out.append(char)
            continue

        if char == '"' and not in_single:
            in_double = not in_double
            out.append(char)
            continue

        if char == "=" and not in_single and not in_double:
            prev_char = text[index - 1] if index > 0 else ""
            next_char = text[index + 1] if index + 1 < length else ""
            if prev_char not in "<>=!" and next_char != "=":
                out.append("==")
                continue

        out.append(char)

    return "".join(out)
