// The differential normalizes the unknown-option TypeError wording of Python and TypeScript. Only that one message shape is
// rewritten, and the option name is kept, so a wrong name or a different message still fails the comparison.
import assert from "node:assert/strict"
import { test } from "node:test"
import { normalize } from "./fixtures/wording.mjs"

test("wording: the Python, Python 3.13 and TypeScript unknown-option messages normalize to the same text", () => {
  const expected = "unknown option 'fusion'"
  assert.equal(normalize("Ranking.__init__() got an unexpected keyword argument 'fusion'"), expected)
  assert.equal(normalize("rank() got an unexpected keyword argument 'fusion'. Did you mean 'fusions'?"), expected)
  assert.equal(normalize("Unknown option 'fusion'"), expected)
})

test("wording: the option name is kept, so different names stay different", () => {
  assert.notEqual(normalize("Unknown option 'a'"), normalize("Unknown option 'b'"))
  assert.notEqual(normalize("f() got an unexpected keyword argument 'a'"), normalize("f() got an unexpected keyword argument 'b'"))
  assert.notEqual(normalize("f() got an unexpected keyword argument 'a'. Did you mean 'c'?"), normalize("Unknown option 'b'"))
  assert.equal(normalize("Unknown option ''"), "unknown option ''")
})

test("wording: a message with anything else around the shape is not rewritten", () => {
  for (const message of [
    "Error: Unknown option 'x'",
    "Unknown option 'x' and more",
    "Unknown option 'x'\nUnknown option 'y'",
    "see f() got an unexpected keyword argument 'x'",
    "f() got an unexpected keyword argument 'x' twice",
    "f() got an unexpected keyword argument 'x'. Did you mean 'y'? Really.",
    "f() got an unexpected keyword argument 'x'. Maybe 'y'?",
    "got an unexpected keyword argument 'x'",
    "f g() got an unexpected keyword argument 'x'",
  ]) assert.equal(normalize(message), message, message)
})

test("wording: the other normalizations stay anchored to their own phrases", () => {
  assert.equal(normalize("Row must be a mapping"), "rows")
  assert.equal(normalize("Rows must be iterable"), "rows")
  assert.equal(normalize("Column 'v' requires a float32 vector"), "Column 'v' float32 vector")
  assert.equal(normalize("exact int values"), "exact T values")
  assert.equal(normalize("exact bigint values"), "exact T values")
  assert.equal(normalize("Primary key must name a str or int column"), "primary key type")
  assert.equal(normalize("Primary key must name a string or bigint column"), "primary key type")
})
