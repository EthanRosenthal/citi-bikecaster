"""Drive the backfill Lambda over a range of months and validate the result.

Resumable: every finished month is appended to a JSONL report and skipped on
the next run.

    # all full months before the cutover month
    uv run scripts/backfill.py run --stage prod --start 2016-09 --end 2026-09
    # the cutover month's days before v2 went live, as daily files
    uv run scripts/backfill.py run --stage prod --month 2026-09 --output daily \
        --days 2026-09-01:2026-09-23
    # compare the report against the legacy S3 row counts and the v2 table
    uv run scripts/backfill.py verify --stage prod
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
import json
from pathlib import Path
import sys

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).parent))
from athena import run as athena  # noqa: E402

TOLERANCE = 0.005


def names(stage: str) -> tuple[str, str]:
    """(resource name prefix, glue database)"""
    return ("citibike", "citibike") if stage == "prod" else (f"citibike-{stage}", f"citibike_{stage}")


def months(start: str, end: str) -> list[str]:
    out, (y, m) = [], map(int, start.split("-"))
    while f"{y:04d}-{m:02d}" < end:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def day_range(spec: str) -> list[str]:
    first, last = (date.fromisoformat(s) for s in spec.split(":"))
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def load_report(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    done = {}
    for line in path.read_text().splitlines():
        rec = json.loads(line)
        if "errorMessage" not in rec:
            done[rec["_job"]] = rec
    return done


def invoke(client, function: str, payload: dict) -> dict:
    resp = client.invoke(FunctionName=function, Payload=json.dumps(payload).encode())
    return json.loads(resp["Payload"].read())


def cmd_run(args):
    report = Path(args.report or f"backfill-{args.stage}.jsonl")
    done = load_report(report)
    fn = f"{names(args.stage)[0]}-backfill"
    client = boto3.client("lambda", config=Config(read_timeout=960, retries={"max_attempts": 0}))

    if args.month:
        jobs = [{"month": args.month, "output": args.output, "days": day_range(args.days) if args.days else None}]
    else:
        jobs = [{"month": m, "output": args.output} for m in months(args.start, args.end)]
    for job in jobs:
        job["source"] = args.source
        job["_job"] = f"{job['month']}:{job['output']}:{job.get('days') and job['days'][0]}"
    todo = [j for j in jobs if j["_job"] not in done]
    print(f"{len(jobs) - len(todo)} already done, {len(todo)} to run via {fn}")

    with ThreadPoolExecutor(args.concurrency) as pool, report.open("a") as out:
        futures = {
            pool.submit(invoke, client, fn, {k: v for k, v in j.items() if v is not None and k != "_job"}): j
            for j in todo
        }
        for fut in as_completed(futures):
            job = futures[fut]
            try:
                rec = fut.result()
            except Exception as exc:  # e.g. client timeout
                rec = {"errorMessage": repr(exc)}
            rec["_job"] = job["_job"]
            out.write(json.dumps(rec) + "\n")
            out.flush()
            if "errorMessage" in rec:
                print(f"{job['month']}: ERROR {rec['errorMessage']}")
            else:
                flags = [d["day"] for d in rec["per_day"] if d.get("flag")]
                print(f"{job['month']}: {rec['rows']:>11,} rows {rec.get('bytes', 0) / 1e6:7.1f} MB flagged={flags}")


def cmd_verify(args):
    report = Path(args.report or f"backfill-{args.stage}.jsonl")
    done = load_report(report)
    problems, per_month, chosen, flagged = [], {}, {}, []
    for rec in done.values():
        per_month[rec["month"]] = per_month.get(rec["month"], 0) + rec["rows"]
        for d in rec["per_day"]:
            chosen[d["chosen"]] = chosen.get(d["chosen"], 0) + 1
            if d.get("flag"):
                flagged.append(d["day"])
            hourly = d.get("hourly_rows")
            if hourly is None:
                continue
            if d["chosen"] == "legacy_hourly" and d["rows"] != hourly:
                problems.append(f"{d['day']}: rows {d['rows']} != hourly {hourly}")
            if d["chosen"] == "legacy_raw" and abs(d["rows"] - hourly) > TOLERANCE * hourly:
                problems.append(f"{d['day']}: raw rows {d['rows']} vs hourly {hourly}")
            if d["chosen"] is None and hourly:
                problems.append(f"{d['day']}: no output but {hourly} hourly rows")

    _, db = names(args.stage)
    rows = athena(
        f"SELECT month, count(*) FROM {db}.station_status GROUP BY 1",
        workgroup=names(args.stage)[0],
    )[1:]
    in_table = {m: int(n) for m, n in rows}
    for month, n in sorted(per_month.items()):
        # The cutover month also holds live data, so it can only be larger.
        if in_table.get(month, 0) < n or (in_table.get(month) != n and not args.allow_live_months):
            problems.append(f"{month}: table has {in_table.get(month, 0)} rows, backfill wrote {n}")

    print(f"months: {len(per_month)}  rows: {sum(per_month.values()):,}  days by source: {chosen}")
    print(f"flagged days ({len(flagged)}): {sorted(flagged)}")
    print("\n".join(problems) or "OK: every day matches the legacy S3 row counts and the v2 table")
    sys.exit(1 if problems else 0)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(required=True)
    r = sub.add_parser("run")
    r.add_argument("--stage", default="dev")
    r.add_argument("--start", default="2016-09")
    r.add_argument("--end", help="exclusive YYYY-MM")
    r.add_argument("--month")
    r.add_argument("--days", help="FIRST:LAST (inclusive) with --month")
    r.add_argument("--output", default="monthly", choices=["monthly", "daily"])
    r.add_argument("--source", default="auto", choices=["auto", "legacy_raw", "legacy_hourly"])
    r.add_argument("--concurrency", type=int, default=8)
    r.add_argument("--report")
    r.set_defaults(func=cmd_run)
    v = sub.add_parser("verify")
    v.add_argument("--stage", default="dev")
    v.add_argument("--report")
    v.add_argument("--allow-live-months", action="store_true",
                   help="allow months that also contain live v2 rows to exceed the backfill count")
    v.set_defaults(func=cmd_verify)
    args = p.parse_args()
    if getattr(args, "func", None) is cmd_run and not (args.month or args.end):
        p.error("run needs --end or --month")
    args.func(args)


if __name__ == "__main__":
    main()
