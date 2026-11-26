// Runs the adversarial scenarios through both SDKs and compares request bytes, results and error classes (VEC-G12).
// Set NABEDRA_TEST_PYTHON to a Python that can import nabedra, or to any Python 3.11 (PYTHONPATH is added here).
import assert from "node:assert/strict"
import { execFileSync } from "node:child_process"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { runAll } from "./fixtures/scenarioRunner.mjs"

const python = process.env.NABEDRA_TEST_PYTHON
const root = fileURLToPath(new URL("../../python/", import.meta.url))

// Scenarios whose outcome differs by the language's own types, with the reason.
const language = new Map([])

// Messages that name a language's own types.
const wording = [
  [/exact (?:int|str|bigint|string) values/, "exact T values"],
  [/requires a float32 vector/, "float32 vector"],
  [/requires finite float32 values/, "float32 vector"],
  [/Row must be a mapping|Rows must be iterable/, "rows"],
  [/Primary key must name a (?:str or int|string or bigint) column/, "primary key type"],
  // An unknown option name is a TypeError in both. Python's interpreter words it, so only the class matters.
  [/^\S+\(\) got an unexpected keyword argument '[^']*'$|^Unknown option '[^']*'$/, "unknown option"],
]
const normalize = (text) => wording.reduce((result, [pattern, replacement]) => result.replace(pattern, replacement), text)

test("the Python and TypeScript SDKs agree on every adversarial scenario", { skip: !python, timeout: 600_000 }, async (t) => {
  const transcript = JSON.parse(execFileSync(python, ["tests/support/scenario_runner.py"], {
    cwd: root, env: { ...process.env, PYTHONPATH: `${root}src` }, maxBuffer: 1 << 30, stdio: ["ignore", "pipe", "ignore"],
  }).toString("utf8"))
  const { scenarios, transcript: own } = await runAll()
  assert.equal(Object.keys(transcript).length, scenarios.length)
  for (const { name } of scenarios) {
    if (language.has(name)) continue
    await t.test(name, () => {
      const { result: python_, ...left } = transcript[name]
      const { result: typescript, ...right } = own[name]
      assert.deepEqual(left, right)
      const [a, b] = [python_.error, typescript.error]
      if (a && b) {
        const { message: pythonMessage, ...pythonError } = a
        const { message: typescriptMessage, ...typescriptError } = b
        assert.deepEqual(pythonError, typescriptError)
        assert.equal(normalize(pythonMessage), normalize(typescriptMessage))
      } else {
        assert.deepEqual(python_, typescript)
      }
    })
  }
})
