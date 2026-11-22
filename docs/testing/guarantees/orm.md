# ORM guarantees

Module: orm
Level: L1

Python and TypeScript follow their own language conventions for the same
shared API. Compare public signatures with the API manifest and replay
shared program and response vectors through both clients. The version check
uses the manifests and built packages and has no numbered scenario.
See the [ORM parity contract](../orm-parity.md).

| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| ORM-G01 | All supported ORMs expose equivalent shared APIs, including options, defaults, types, and return values. | A release can omit an export or change a signature, default, or type in one language. | O01, D01 | U D | M: Public signatures are extracted directly; S: Signature checks need no simulated faults; H: The manifest compares public members directly; F: The check concerns API declarations; R: The signature comparison runs on the clients without a server; P: API parity has no performance target; K: A signature check compares the complete API |
| ORM-G02 | The same program produces identical results, side effects, and error meanings in all supported ORMs. | Language decoders can diverge on typed values, malformed replies, error envelopes, and reconnects. | O01, D01, K01, ST04 | D F R | M: The same program and expected exchange give the oracle; U: The shared vectors and public entry points are exercised by D and F; S: The end-to-end cases use a running server; H: Result, error, and exchange parity are compared directly or in the end-to-end run; P: Parity covers behavior; K: Finite shared programs cover the exchanges |
| ORM-G03 | All supported ORMs declare, build, and publish the same version in lockstep. | A manifest, built package, or published package can retain a different version. | | U D | M: Version equality is a direct assertion; S: Package metadata has no simulated fault model; H: The check compares versions directly; F: Version fields are read from package metadata; R: Manifests and built artifacts can be inspected without a server; P: Version equality has no performance target; K: One comparison covers the release set |
