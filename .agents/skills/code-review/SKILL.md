---
name: code-review
description: Use when asked to do QA, code review or testing
---

When you are asked to review a PR, an implementation or the like, do the following:

0. Make the absolute most fundamental rigorous checks.
Here goes making sure the code compiles, it has no syntax errors, etc.

1. Come up with the 20% of tests that will assess 99% of the quality.
Create a rubric with the minimal set of tests that comprises a full quality assessment of the changes made that rigorously make sure there are no bugs, errors, unintended behavior or efficiency issues. Do the following and include it in the PR or response to the user that asked your code review:
- When a change is made to the UI, take a screenshot or record a video (if it includes animations or other moving parts) of the before and after. Use it to compare if the new UI renders correctly, has not padding/margin issues, font style issues, and perform style checks for the changed components. Of course, also make sure that nothing in the UI crashes or gives dramatic errors under plausible user interactions. 
- When a change is made to the logic, test the main use cases at different scales. If a module for a database is coded, test it for a myriad of possible data and at different possible scales; for example, inserting 1,000 rows vs 10,000. NOTE: Ask for CPU and RAM budgets if you are not given them, and use realistic numbers. It might not be realistic for a testing to run a batch of a billion inserts, but it might be sensible to run a batch of 10, then 100, then 1,000. Here what you want to test is the scaling capability and that the system does not break down at: (1) all its intended scales, (2) all its intended input and output data types [if it is some regex module, try out with strings that are expected and unexpected, those that the code planned for and it didn't plan for...], (3) all intended interactions between modules [e.g., a query that uses multiple intrincate filters in a complicated order, multiple workers handling a big query and how those workers map and reduce the data, if they give any bug, crash, inefficiency or malformed data, that the loaded data fits on the RAM of the device, that CPU demands don't exceed the available CPU...]
- In either case, test the efficiency of the implementation by checking unnecessary renders in the case of the UI (e.g., too many rerenderings for a page or component, too much time for the page to first load, too much lag between interactions and page transitions...) and unnecessary or suboptimal functionality for logic (e.g., laggish queries, laggish joins, laggish pipelines or data processing...)
- In either case, also test the main edge cases that users will most likely face. E.g., clicking on buttons, sliding or scrolling for UIs; scaling to multiple workers, doing complicated queries to a db or doing Stripe transactions in erratic manners for logic.

2. Regression tests
Make sure the new changes do not break anything of what we previously had. This means that the new implementation of a sidebar or module does not break the dashboard, bugs a module already implemented, etc.

For ORM changes and database changes that affect ORM callers, include the
[ORM parity checks](../../../docs/testing/orm-parity.md) in the review rubric.

If there is some compatibility issue (i.e., a change of specs; e.g., migrating from SQLite local db in a mobile phone to JSON files in the app data folder), ask the user if it is intended to break regression compatibility and thus not work well in previous implementations. If not intended, e.g. perhaps losing the previous data, act in consequence. Anyway make sure to ask the user if backwards compatibility is expected to be retained or not, because it might depend on the project (it's not the same a beta app used by virtually no one versus a production and famous mobile app)

3. ZERO Slop policy
Do not overcheck. Never ever create AI slop tests that do not add a ton of value to the code and quality review. These are relatively useless because compilation, syntax errors, etc. are already covered in point 0 and in order to make the actually valuable tests (those that imply user interaction with the UI, intercommunication between logic modules, scaling, AWS/Stripe/Third party integration and API usage, memory/CPU/GPU usage...) the "fundamentals" of the repo (it complies, etc) must indeed work well. 

Do check point 0's most fundamental checks for compilation, syntax errors, plausible errors that might occur during code execution in some edge case scenario...

4. Work in realistic environments
Use a realistic test environment that fits the workload and reproduces how
users run the system. For remote builds, heavy Docker and Kubernetes checks,
and authorized S3 testing, follow the
[Nabedra builds skill](../nabedra-builds/SKILL.md).

Run full suites, acceptance, campaigns and image checks on GitHub Actions by
deliberate dispatch pinned to the reviewed commit, following
[GitHub Actions for checks](references/gh-actions.md). Pushes start nothing.
A check passes only when its run URL, tested SHA and executed assertions are
recorded. List not-run and quarantined checks separately; never count them as
passing.

# Number of tests
Aim for 3-10 big essential tests/categories max per functionality/use case category and then do some variations of each, 3-5 per category. Each essential test should amount to a big use or edge case that needs to be tested. Use `references/example.md` as a reference: there we have 5 big use case categories, 2-3 essential tests/categories, then in the implementation YOU should make variations of each of those bulleted-points essential tests. E.g., to check that the queue doesn't break under scaling (one possible essential test for the scaling use case) you'd check with 3-5 different configurations (request size, number of parallel requests, when they burst...) 

# Rubric-style reviewing

When you are asked to make a code/QA review/testing, start by listing in the form of tasks or a TODO table in markdown all tests you are going to make according to the guidelines above. Then, spawn one agent per testing task. You should expect the user to specify to you the model they wanna use; if not, assume you shall use the latest GPT Sol model on high or the latest Claude Opus model on high, depending on whether you're Codex/GPT or Claude Code. Each subagent shouldn't have to do TOO much but not TOO little either: the goal is to keep mid-size tasks done by subagents to limit context window usage of the orchestration agent (you).

Each test subagent dispatches its own GitHub Actions run with its own request ID, and cancels or reruns only runs it dispatched. Try to spawn as many subagents as possible without interfering with the tests of each other. E.g., if Test 2a has to wait for Test 1a to be done, then don't run a subagent for Test 2a until Test 1a is done; but if Test 1a and Test 1b, and 1c and 1d... can be done in parallel, spawn a subagent for each.

On `references/example.md` you shall find developer test prompts as example lists of relevant tests grouped by use case. Consult the matching use case when choosing tests for a review
