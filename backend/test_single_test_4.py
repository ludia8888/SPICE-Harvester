#!/usr/bin/env python3
import asyncio
from test_ontology_creation_integration import OntologyIntegrationTester

async def run_test_4():
    tester = OntologyIntegrationTester()
    await tester.setup()
    try:
        result = await tester.test_update_and_delete()
        print(f'Test result: {result}')
    finally:
        await tester.teardown()

if __name__ == "__main__":
    asyncio.run(run_test_4())