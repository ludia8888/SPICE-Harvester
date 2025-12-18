# Complex Types Testing Guide (Steady State)

> Updated: 2025-12-17

This guide documents where complex-type validation lives and how to run the current tests.

## Code locations

- Validation: `backend/shared/validators/complex_type_validator.py`
- Serialization helpers: `backend/shared/serializers/complex_type_serializer.py`
- Ontology/type helpers: `backend/shared/models/ontology.py`

## Current test coverage

- Unit validation coverage: `backend/tests/test_core_functionality.py::TestComplexTypes`
- End-to-end ontology flows (type mapping exercised via OMS API): `backend/tests/test_core_functionality.py::TestCoreOntologyManagement`

## Running the tests

### 1) Unit-only (no infra required)

```bash
pytest backend/tests/test_core_functionality.py -k TestComplexTypes -q
```

### 2) Full integration (requires services running)

Start infra + services (TerminusDB/MinIO/Postgres/Kafka, then OMS/BFF/Funnel), then:

```bash
pytest backend/tests/test_core_functionality.py -q
```

## Troubleshooting

### ModuleNotFoundError (optional validators)

```bash
pip install phonenumbers email-validator
```
