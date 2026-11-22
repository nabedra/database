# ORM conformance

Install the TypeScript package's locked dependencies with `npm ci --prefix orm/typescript`. Run these commands from the repository root with Python 3.11 or later.

| Command | Assertion |
| --- | --- |
| `python3.11 scripts/testing/api_manifest.py check` | The signatures and versions match the committed manifest, and every parity gap has a reason. |
| `python3.11 scripts/testing/api_manifest.py parity` | List one-sided symbols in both directions. Exit with status 1 for a new gap or a stale exception. |
| `python3.11 -m unittest scripts.testing.test_api_manifest` | Reject a changed Python default, a new one-sided method and mismatched package versions. |
| `PYTHONPATH=orm/python/src python3.11 -m unittest discover -s orm/python/tests -p test_contract_vectors.py` | Replay OLAP and Events exchanges from Python. |
| `npm run build --prefix orm/typescript && node --test orm/typescript/tests/contract-vectors.test.mjs` | Replay the same exchanges from the TypeScript package. |

## API manifest

The manifest records each public import path for OLTP, OLAP, Cache, Events and streaming. It includes package-root re-exports and the data SDK subpaths. Python uses `__all__` where present and module-owned public definitions elsewhere. Deployment, cluster management, the gateway and the main CLI are outside this manifest.

Each row identifies a module, symbol, kind, parameters and return type. Class and interface members have qualified names such as `Query.orderBy`. Python `snake_case` names become `camelCase` and constants such as `INT64_MIN` become `int64Min`. Acronyms without underscores retain their spelling. Python expression operators map to TypeScript methods such as `__ge__` to `ge`. Stream context entry maps to `open`, iterator hooks map to `iterator` and `next`, and context exit and disposal hooks map to `dispose`. The `nativeSymbol` field retains the original spelling when a name is mapped.

Parameters retain their declared names and types. Python records runtime defaults and overloads from `inspect`. TypeScript records overloads, type parameters, and optional and rest parameters from emitted declarations. TypeScript declaration emission omits runtime default values, so the manifest captures their optionality. The `collect_defaults` and `poll_defaults` vectors omit optional arguments, so a changed runtime default in either ORM changes the request they send. Each language is compared against its own committed signatures. Cross-language membership uses module and symbol because the languages express options, types and asynchronous results differently.

For an approved API change, update both ORMs and run `python3.11 scripts/testing/api_manifest.py generate`. This retains the manifest's explicit allowlist. Each key names the language, module and symbol that currently have a one-sided declaration. Add a reason for an approved language-specific exception and remove it when the counterpart exists.

## Wire vectors

Each case selects an operation and can override its response or decoded result. The operations list the preceding exchanges, the expected request and a default response. Both clients send real HTTP requests to a local fixture. The tests compare method, path, authentication, database and lease headers, and the complete UTF-8 body. The fixture substitutes the generated OLAP request ID for `@request_id` when echoing it back. Response bodies are raw strings so malformed JSON, duplicate fields, and integer-versus-float tokens survive fixture loading.

Expected results use decimal strings for wide integers after asserting the native integer type. Malformed cases pin whether reads report a protocol error and whether writes report an unknown outcome. The OLAP vectors cover the JSON and response-validation clauses of U02. The [Cache contract](../cache-contract/README.md) has its own runner and vectors.
