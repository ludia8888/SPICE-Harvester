from __future__ import annotations

from types import SimpleNamespace

from shared.services.core.graph_federation_service_woql import GraphFederationServiceWOQL


def _make_service() -> GraphFederationServiceWOQL:
    terminus_service = SimpleNamespace(
        connection_info=SimpleNamespace(
            user="u",
            key="k",
            account="acct",
            server_url="http://terminus",
        )
    )
    return GraphFederationServiceWOQL(terminus_service=terminus_service)


def _find_relationship_triples(woql: dict) -> list[dict]:
    # woql is a Select {query: {and: [...]}}
    query = woql.get("query") or {}
    and_terms = (query.get("and") or []) if isinstance(query, dict) else []
    triples = [t for t in and_terms if isinstance(t, dict) and t.get("@type") == "Triple"]
    # Filter out rdf:type checks and keep only relationship triples.
    return [t for t in triples if (t.get("predicate") or {}).get("node") != "rdf:type"]


def test_build_multi_hop_woql_reverse_hop_flips_triple_direction() -> None:
    svc = _make_service()
    woql, hop_vars = svc._build_multi_hop_woql(
        "Person",
        [("owner", "BankAccount", True)],
        filters={"phone": "010-1234-5678"},
    )

    assert hop_vars and hop_vars[0][0] == "owner"
    assert hop_vars[0][1] == "BankAccount"
    assert hop_vars[0][3] is True

    rel_triples = _find_relationship_triples(woql)
    assert rel_triples, "Expected at least one relationship triple"
    rel = next((t for t in rel_triples if (t.get("predicate") or {}).get("node") == "@schema:owner"), None)
    assert rel is not None
    assert (rel.get("subject") or {}).get("variable") == "v:RelatedBankAccount"
    assert (rel.get("object") or {}).get("variable") == "v:Person"


def test_build_multi_hop_woql_forward_hop_keeps_triple_direction() -> None:
    svc = _make_service()
    woql, hop_vars = svc._build_multi_hop_woql(
        "Person",
        [("accounts", "BankAccount", False)],
        filters=None,
    )

    assert hop_vars and hop_vars[0][0] == "accounts"
    assert hop_vars[0][3] is False

    rel_triples = _find_relationship_triples(woql)
    rel = next((t for t in rel_triples if (t.get("predicate") or {}).get("node") == "@schema:accounts"), None)
    assert rel is not None
    assert (rel.get("subject") or {}).get("variable") == "v:Person"
    assert (rel.get("object") or {}).get("variable") == "v:RelatedBankAccount"

