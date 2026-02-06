from __future__ import annotations


_INVERSE_CARDINALITY_MAP = {
    "1:1": "1:1",
    "1:n": "n:1",
    "n:1": "1:n",
    "n:m": "n:m",
    "one": "many",
    "many": "one",
}


def inverse_cardinality(cardinality: str) -> str:
    return _INVERSE_CARDINALITY_MAP.get(cardinality, cardinality)
