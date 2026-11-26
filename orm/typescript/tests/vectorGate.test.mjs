// Tests added from the task D mutation gate.
import assert from "node:assert/strict"
import { test } from "node:test"
import { ValidationError, col, list, vector } from "nabedra"
import { expressionToWire } from "../dist/expr.js"

const schema = { id: String, vec: vector(2), tags: list(String) }

test("containsAny refuses a Map itself, so only a Set is ever sorted", () => {
  // A Map yields [key, value] pairs. They are not strings, so no order rule can apply to them.
  assert.throws(() => col("tags").containsAny(new Map([["b", 1], ["a", 2]])), ValidationError)
  assert.deepEqual(expressionToWire(col("tags").containsAny(new Map([["b", 1], ["a", 2]]).keys()), schema).values, ["b", "a"])
})
