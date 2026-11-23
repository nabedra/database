# Nabedra logic tests

Golden-file tests for the JSON operations API (`POST /v1/operations`). The
format follows sqllogictest, but each command is one JSON operation instead of
SQL, because the engine has no SQL.

Three runners read the same `.nlt` files and must render results identically:

| Runner | Where | How it runs |
| --- | --- | --- |
| Rust | `server/tests/olap_logic.rs` | In process, through the real `/v1/operations` router over an in-memory object store. One fresh engine per file. |
| Python ORM | `orm/python/tests/test_logic_replay.py` | Over HTTP through the ORM transport, against `NABEDRA_LOGIC_URL`. |
| TypeScript ORM | `orm/typescript/tests/logic-replay.test.mjs` | Over HTTP through the ORM transport, against `NABEDRA_LOGIC_URL`. |

Run the Rust runner with `cargo test --test olap_logic -- --nocapture` from
`server/`. Set `NABEDRA_LOGIC_FILTER=<substring>` to run only matching files.
Set `NABEDRA_BLESS=1` to rewrite every expected block in place with the actual
output for each file whose statements and labels pass. Review blessed diffs by
hand before committing. The runner prints the corpus assertion count as
`logic result:`. Cargo reports the Rust test count separately.

The ORM replays skip with `not-run: NABEDRA_LOGIC_URL unset` when no server is
given. `NABEDRA_LOGIC_TOKEN` (default `NABEDRA_TOKEN`) is the bearer token.
`NABEDRA_LOGIC_ENGINE` (`olap` by default) selects which `require` sets run.
Use a disposable database. The tests create tables and exercise mutations and
deletes. The registered ORM checks run the HTTP replay only.

Without a server, the renderers can still be compared on real engine output.
`NABEDRA_LOGIC_RECORD=<dir>` makes the Rust runner write every exchange of each
file to `<dir>/<file>.nlt.json`. `NABEDRA_LOGIC_TRANSCRIPTS=<dir>` makes both ORM
tests re-render the whole corpus from those responses, decoded as their
transports decode them, and compare with the same goldens. Without it, that test
skips with `not-run: NABEDRA_LOGIC_TRANSCRIPTS unset`.

## Records

A file is a sequence of records separated by blank lines. Lines that start
with `#` outside a record are comments. The first line is `# name: <name>`.

| Directive | Meaning |
| --- | --- |
| `# name: <name>` | Required first line. A human name for the file. |
| `require olap`, `require oltp`, `require olap oltp` | File level, before the first record. Engines the file applies to. Without it, the file applies to both. |
| `skipif <reason>` | On the line right before a record header. The record is parsed but not executed. Runners list it as skipped with the reason. Use it for a suspected product bug, never to hide a wrong golden. |
| `statement ok` | The operation must succeed. Its result is ignored. |
| `statement error <pattern>` | The operation must fail. `<pattern>` matches when it equals the error code (such as `not_found`) or is a substring of the error message. |
| `query rowsort\|nosort [hash] [label-<name>]` | The operation must succeed and its rendered rows must equal the expected block. `rowsort` sorts both sides first. `nosort` compares in result order. |
| `explain` | A `query.explain` operation. Each plan line is one row, compared in order, after ID normalization. |

The operation is the JSON text on the lines after the header, up to a blank
line (for statements) or a `----` line (for queries and explain). It may span
several lines.

For `query` and `explain`, the expected rows follow `----`, up to a blank line
or the end of the file. Values in a row are separated by one tab.

### Hash

With `hash`, the expected block is one line, `<N> values hashing to <hex>`.
`N` is the number of values (rows times columns). `<hex>` is the lowercase
SHA-256 of every value, after sorting, each followed by `\n`, in UTF-8.

### Labels

Every query with the same `label-<name>` in a file must render the same rows
(after its own sort mode). A labelled query may omit its `----` block, in which
case only the agreement is checked. Use labels to assert that two different
operations agree without writing the answer down.

## Operation translation

Files name tables, not IDs. Each runner turns the JSON object into a wire
request the same way:

1. `op` is the operation name. The rest of the object is the payload.
2. `table` (a string) is removed and replaced by `table_id`, the ID the runner
   recorded when that table was created in this file. An unknown name becomes
   `ffffffff-ffff-4fff-bfff-ffffffffffff`, which never exists, so a bad table
   gives `not_found`. A dropped table keeps its old ID.
3. For `table.create` and `table.describe`, the ORM runners prefix valid table
   names with a random prefix per file. If the result would exceed 64 characters,
   they shorten the name and include a hash within the 64-character limit.
   Invalid names go to the server unchanged so the server validates them.
   The Rust runner uses one fresh engine per file and sends every name as written.
4. For `query.collect`, `query.count` and `query.explain` without a `query`
   key, the runner moves `select`, `filter`, `order_by` and `limit` into
   `query`, and sets each missing one to `null`. With a `query` key, the payload
   is sent as written, which lets a file test malformed queries.

A successful `table.create` records the table ID under the logical name.

## Result rendering

Each operation result becomes a list of rows of strings:

| Operation | Rows |
| --- | --- |
| `table.create`, `table.describe` | One row: logical name, key, schema. |
| `table.list` | One row per table whose name has the runner's prefix, sorted by name. |
| `table.get` | No rows for `null`, otherwise one row like `query.collect`. |
| `query.collect` | One row per result row. The runners check that each row has exactly the selected columns and render them in the order of `select`. Without a selection, they render columns in ascending name order. |
| `query.count` | One row with the count. |
| `query.explain` | One row per plan line. |
| Mutations and `table.drop` | No rows for `null`, otherwise one row with the value (a count or a found flag). |

The schema renders as `col:type` pairs in ascending column order, joined by
`,`. A logical name is the physical name with the prefix removed.

Values render as follows:

| Value | Rendering |
| --- | --- |
| JSON `null` | `NULL` |
| Boolean | `true`, `false` |
| Integer | Decimal digits, with `-` when negative |
| Float | Canonical float, below |
| String | `(empty)` for the empty string. Otherwise the string, with `\` as `\\`, tab as `\t`, newline as `\n`, carriage return as `\r`, and any other character below U+0020 or U+007F as `\xHH` (lowercase hex) |

A canonical float uses the shortest digits that round-trip, `d1 d2 ... dn`,
and the decimal exponent `x` of `d1`. For equally close representations of
the same length, the decimal with an even last digit wins. Zero of either sign
is `0.0`. A negative value gets a leading `-`. When `-7 < x < 21` the float is plain decimal with at
least one fractional digit (`2.0`, `0.001`, `1.5`). Otherwise it is
`d1[.d2...dn]e<x>`, such as `1e21` or `1.5e-7`.

`rowsort` orders rows by their tab-joined text in UTF-8 byte order.

Explain normalization replaces every UUID with `<uuid>` and restores the
logical table name on the scan line.

## Notes on the data model

The wire format has no NULL values. A row must contain every schema column,
and a filter value must be a non-null scalar of the column's exact type. NULL
tests therefore check that null is rejected, and `NULL` appears in output only
for absent JSON values. Comparisons follow `server/protocol/src/query_semantics.rs`:
strings compare by bytes, `false < true`, floats compare numerically with
`-0.0 == 0.0`, and every sort breaks ties by the primary key. There are no
aggregates other than `query.count` and no group-by.
