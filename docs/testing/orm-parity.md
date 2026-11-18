# ORM parity

The Python and TypeScript ORMs, and every future supported ORM, must provide the
same shared API and behavior using each language's conventions. When a task
adds, changes, removes, or fixes a shared capability, carry the change and its
tests across all implementations in the same PR. This also applies to server
changes that affect what callers can do or observe through an ORM.

1. **API.** Compare the public package imports, functions, classes, objects,
   methods, properties, and types in both directions. Each shared member must
   have a counterpart with equivalent inputs, options, defaults, and return
   values. Use each language's naming, types, and calling conventions. Python's
   `insert_many` and TypeScript's `insertMany` are the same operation.
2. **Behavior.** Run the same relevant success, edge, and error cases through
   each ORM's public entry points. Compare results, side effects, and error
   meanings, using the same fixtures and server version for server-backed cases.
   Reuse the [export comparison](../../orm/typescript/tests/api.test.mjs) and
   [request comparison](../../orm/typescript/tests/query.test.mjs) where relevant.
   Set `NABEDRA_TEST_PYTHON` to enable their Python comparisons.
3. **Versions.** Check that all ORM manifests declare the same version. Bump
   them together in the same PR, including for a language-specific change that
   warrants a release. Verify that the built packages carry that version and
   publish the ORM packages together. If Python releases `1.0.1`, TypeScript
   and every other supported ORM release `1.0.1` too.

Keep integrations that are useful in a particular language. Where another
language has a suitable native or ecosystem equivalent, adapt the integration
and test the corresponding behavior there. For example, a Python `to_numpy`
helper could have a TypeScript conversion to a typed array if it preserves the
data and semantics callers need. Where no suitable equivalent exists, keep the
helper in the languages that support it. Document the exception, which
languages it applies to, and why. The shared operation that supplies the data
still needs parity.

Include the implementations checked, any documented exceptions, and the test
results in the PR. Treat an unexplained mismatch as a review blocker. Report
skipped or unavailable checks as unverified.
