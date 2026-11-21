# LIVE3 OLAP provider checks in local Docker

The local provider fixture starts real API and worker processes inside one
Linux container. Its callbacks provision and terminate processes and inject
an inventory failure. The stream oracle reopens the original handle after
worker removal, verifies the byte-identical replay, then reads the remaining
row and EOF. This fixture exercises the A01, A02 and ST05 callback and drain
assertions. Cloud provider checks stay in the
[LIVE2 preflight](../live-aws/README.md).

Run from the repository root using the verified `live3-olap` artifact.

```sh
PYTHONDONTWRITEBYTECODE=1 python3.11 scripts/local-kind/docker_checks.py olap \
  --binary "$LIVE3_WORK/live3-olap/nabedra-db" --revision "$LIVE3_REVISION" \
  --lane "$LIVE3_LANE" --output "$LIVE3_WORK/olap"
```

## Results

| Check | Status | Executed assertion |
| --- | --- | --- |
| A01 | Passed | The manual API refused autoscaling and its worker counts stayed at zero. |
| A02 | Passed | Scale-up reached two ready workers. An inventory failure preserved eight accepted requests. Four more requests settled across worker removal. Fixed scale-to-zero and restart retained the committed row and changed the run. |
| ST05/A02 | Passed | The original stream replayed batch zero byte for byte after the drain. It then returned the exact next row and EOF. |
| AWS provider topology | Not run | LIVE3 uses local Docker with an isolated network. |

The Linux OLAP release binary at `a5ebd056de0cafba437637f657c90ed724246ba7`
had SHA-256 `98da8fc1616168f3de6c8a7b47373e3f8f3a2eee03292394ee501f8a5a77ede9`.
The container exited zero after 20.35 seconds. Peak observed Docker VM use was
787,861,504 bytes. Cleanup confirmed the container was absent before the
Docker lane was released.
[Raw assertions](olap-output.txt) · [Session trace](trace.jsonl) ·
[Linux build](https://github.com/zPy52/nabedra-database/actions/runs/44923954740)
