"""Summarizes filter-cost.sh output: `explain LABEL` reads EXPLAIN JSON documents (first run cold, the rest warm) on
stdin; `walk LABEL COLD_MS` reads `batches matched ms` lines from the warm runs."""
import json
import statistics
import sys


def nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from nodes(child)


def explain(label):
    text, decoder, plans, i = sys.stdin.read(), json.JSONDecoder(), [], 0
    while i < len(text):
        if text[i] == "[":
            doc, i = decoder.raw_decode(text, i)
            plans.append(doc[0])
        else:
            i += 1
    first, warm = plans[0], plans[1:]
    top = first["Plan"]
    scans = ",".join(
        n["Node Type"] + (f"({n['Index Name']})" if "Index Name" in n else "")
        for n in nodes(top) if "Scan" in n["Node Type"])
    warm_ms = statistics.median(w["Execution Time"] for w in warm)
    plan_ms = statistics.median(w["Planning Time"] for w in warm)
    buffers = top["Shared Hit Blocks"] + top["Shared Read Blocks"]
    print(f"{label:<32} rows={top['Actual Rows']:<4} cold_ms={first['Execution Time']:>8.1f} "
          f"warm_ms={warm_ms:>8.1f} plan_ms={plan_ms:5.2f} buffers={buffers:<6} {scans}")


def walk(label, cold_ms):
    rows = [line.split() for line in sys.stdin if line.strip()]
    warm_ms = statistics.median(float(r[2]) for r in rows)
    print(f"{label:<32} batches={rows[0][0]} matched={rows[0][1]} cold_ms={float(cold_ms):>8.1f} warm_ms={warm_ms:>8.1f}")


if sys.argv[1] == "explain":
    explain(sys.argv[2])
else:
    walk(sys.argv[2], sys.argv[3])
