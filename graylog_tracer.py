#!/usr/bin/env python3
"""
Graylog issue tracer: search error logs in a date/time interval and output JSON
with timestamps in Asia/Tehran and all message fields.
"""
import argparse
import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

TEHRAN = ZoneInfo("Asia/Tehran")
UTC = ZoneInfo("UTC")

# Graylog API endpoints (scripting API is newer; universal/absolute is legacy)
SEARCH_MESSAGES_URL = "/api/search/messages"
SEARCH_UNIVERSAL_ABSOLUTE_URL = "/api/search/universal/absolute"
PAGE_SIZE = 500
# Fields to request when API requires them; empty means "all" for some Graylog versions
DEFAULT_FIELDS = [
    "timestamp",
    "message",
    "source",
    "level",
    "type",
    "data_url_host",
    "url_host",
    "data_message_exception",
    "response_status",
    "host",
    "gl2_source_input",
    "gl2_source_node",
]


# Section keys in .env and in output
SECTION_BACKEND_MOBAPI = "backend_mobapi"
SECTION_FRONTEND_NEXTJS = "frontend_nextjs"
SECTION_FRONTEND_NEXTJS_PODS = "frontend_nextjs_pods"

ENV_QUERIES_BY_SECTION = {
    SECTION_BACKEND_MOBAPI: "GRAYLOG_QUERIES_BACKEND_MOBAPI",
    SECTION_FRONTEND_NEXTJS: "GRAYLOG_QUERIES_FRONTEND_NEXTJS",
    SECTION_FRONTEND_NEXTJS_PODS: "GRAYLOG_QUERIES_FRONTEND_NEXTJS_PODS",
}


def _parse_queries_env(raw: str, env_key: str) -> list[str]:
    try:
        queries = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid {env_key} in .env (must be JSON array): {e}")
    if not isinstance(queries, list) or not all(isinstance(q, str) for q in queries):
        raise SystemExit(f"{env_key} must be a JSON array of strings")
    return queries


def _parse_time_frame_seconds(s: str) -> int:
    """Parse GRAYLOG_QUERIES_FRONTEND_NEXTJS_PODS_TIME_FRAME e.g. '4s', '1m', '2h' -> seconds."""
    s = (s or "").strip().lower()
    if not s:
        return 0
    if s.endswith("s"):
        return int(s[:-1].strip() or 0)
    if s.endswith("m"):
        return int((s[:-1].strip() or 0)) * 60
    if s.endswith("h"):
        return int((s[:-1].strip() or 0)) * 3600
    return int(s)


def load_config():
    load_dotenv()
    domain = os.getenv("GRAYLOG_DOMAIN", "").rstrip("/")
    username = os.getenv("GRAYLOG_USERNAME")
    password = os.getenv("GRAYLOG_PASSWORD")
    if not domain or not username or not password:
        raise SystemExit("Set GRAYLOG_DOMAIN, GRAYLOG_USERNAME, GRAYLOG_PASSWORD in .env")
    sections = {}
    for section_key, env_key in ENV_QUERIES_BY_SECTION.items():
        raw = os.getenv(env_key, "[]")
        sections[section_key] = _parse_queries_env(raw, env_key)
    start_date = (os.getenv("START_DATE") or "").strip()
    start_time = (os.getenv("START_TIME") or "00:00").strip()
    end_date = (os.getenv("END_DATE") or "").strip()
    end_time = (os.getenv("END_TIME") or "23:59").strip()
    output_fields_raw = os.getenv("GRAYLOG_OUTPUT_FIELDS", "[]")
    try:
        output_fields = json.loads(output_fields_raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid GRAYLOG_OUTPUT_FIELDS in .env (must be JSON array): {e}")
    if not isinstance(output_fields, list) or not all(isinstance(f, str) for f in output_fields):
        raise SystemExit("GRAYLOG_OUTPUT_FIELDS must be a JSON array of strings")
    output_fields_set = frozenset(output_fields)
    filter_keywords_raw = os.getenv("GRAYLOG_FILTER_KEYWORDS", "[]")
    try:
        filter_keywords = json.loads(filter_keywords_raw)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid GRAYLOG_FILTER_KEYWORDS in .env (must be JSON array): {e}")
    if not isinstance(filter_keywords, list) or not all(isinstance(k, str) for k in filter_keywords):
        raise SystemExit("GRAYLOG_FILTER_KEYWORDS must be a JSON array of strings")
    filter_keywords_lower = [k.strip().lower() for k in filter_keywords if k.strip()]
    # Time frame for frontend_nextjs_pods aggregation (e.g. "4s" -> 4 seconds)
    tf_raw = (os.getenv("GRAYLOG_QUERIES_FRONTEND_NEXTJS_PODS_TIME_FRAME") or "").strip()
    pods_time_frame_seconds = _parse_time_frame_seconds(tf_raw) if tf_raw else 0
    return {
        "base_url": domain,
        "auth": (username, password),
        "sections": sections,
        "start_date": start_date,
        "start_time": start_time,
        "end_date": end_date,
        "end_time": end_time,
        "output_fields": output_fields_set,
        "filter_keywords": filter_keywords_lower,
        "frontend_nextjs_pods_time_frame_seconds": pods_time_frame_seconds,
    }


def parse_datetime(s: str, tz: ZoneInfo) -> datetime:
    """Parse datetime string; assume ISO or 'YYYY-MM-DD HH:MM' in given tz."""
    s = s.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s.replace("Z", "").split("+")[0].strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse datetime: {s!r}")


def _parse_date_time(date_str: str, time_str: str) -> datetime:
    """Parse START_DATE + START_TIME (or END_*) into a datetime in Tehran. Accepts HH:MM or HH:MM:SS."""
    combined = f"{date_str.strip()} {time_str.strip()}"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(combined, fmt).replace(tzinfo=TEHRAN)
        except ValueError:
            continue
    raise ValueError(f"Invalid date/time: {combined!r}. Use YYYY-MM-DD and HH:MM or HH:MM:SS.")


def parse_interval_from_env(config: dict) -> tuple[datetime, datetime]:
    """Build from/to datetimes in Asia/Tehran from START_DATE, START_TIME, END_DATE, END_TIME."""
    for key in ("start_date", "start_time", "end_date", "end_time"):
        if not (config.get(key) or "").strip():
            raise SystemExit(
                "Set START_DATE, START_TIME, END_DATE, END_TIME in .env, or use --from and --to"
            )
    try:
        from_dt = _parse_date_time(config["start_date"], config["start_time"])
        to_dt = _parse_date_time(config["end_date"], config["end_time"])
        return from_dt, to_dt
    except ValueError as e:
        raise SystemExit(str(e))


def to_tehran_iso(dt: datetime) -> str:
    return dt.astimezone(TEHRAN).strftime("%Y-%m-%dT%H:%M:%S.%f%z")


def format_timestamp_tehran(dt: datetime) -> str:
    """Format as 2026-02-18 19:46:13 +0330/Tehran"""
    return dt.astimezone(TEHRAN).strftime("%Y-%m-%d %H:%M:%S +0330/Tehran")


def _search_graylog_scripting(
    base_url: str, auth: tuple, query: str, from_utc: datetime, to_utc: datetime, fields: list | None
) -> tuple[list[str], list[list]]:
    """Use POST /api/search/messages (Search Scripting API). Returns (schema_fields, rows)."""
    url = f"{base_url.rstrip('/')}{SEARCH_MESSAGES_URL}"
    from_iso = from_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_iso = to_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    timerange = {"type": "absolute", "from": from_iso, "to": to_iso}
    payload = {
        "query": query,
        "timerange": timerange,
        "from": 0,
        "size": PAGE_SIZE,
        "sort": "timestamp",
        "sort_order": "desc",
    }
    if fields is not None:
        payload["fields"] = fields

    all_rows = []
    schema_fields = []
    offset = 0

    while True:
        payload["from"] = offset
        resp = requests.post(
            url,
            json=payload,
            auth=auth,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Requested-By": "issue-tracer",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Graylog API error {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        schema = data.get("schema", [])
        datarows = data.get("datarows", [])
        if not schema_fields and schema:
            schema_fields[:] = [c.get("field") or c.get("name", "") for c in schema]
        for row in datarows:
            all_rows.append(row)
        if len(datarows) < PAGE_SIZE:
            break
        offset += len(datarows)

    return schema_fields, all_rows


def _search_graylog_legacy(
    base_url: str, auth: tuple, query: str, from_utc: datetime, to_utc: datetime, _fields: list | None
) -> tuple[list[str], list[list]]:
    """Use GET /api/search/universal/absolute (legacy). Returns (schema_fields, rows)."""
    url = f"{base_url.rstrip('/')}{SEARCH_UNIVERSAL_ABSOLUTE_URL}"
    from_iso = from_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to_iso = to_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params = {
        "query": query,
        "from": from_iso,
        "to": to_iso,
        "limit": PAGE_SIZE,
        "sort": "timestamp",
        "order": "desc",
    }
    all_rows = []
    schema_fields = []
    offset = 0

    while True:
        params["offset"] = offset
        resp = requests.get(
            url,
            params=params,
            auth=auth,
            headers={"Accept": "application/json", "X-Requested-By": "issue-tracer"},
            timeout=60,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Graylog API error {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        messages = data.get("messages", [])
        for m in messages:
            msg_obj = m.get("message", m)
            fields_dict = msg_obj.get("fields", msg_obj) if isinstance(msg_obj, dict) else msg_obj
            if not isinstance(fields_dict, dict):
                continue
            if not schema_fields:
                schema_fields[:] = list(fields_dict.keys())
            row = [fields_dict.get(f) for f in schema_fields]
            all_rows.append(row)
        if len(messages) < PAGE_SIZE:
            break
        offset += len(messages)

    return schema_fields, all_rows


def search_graylog(base_url: str, auth: tuple, query: str, from_utc: datetime, to_utc: datetime, fields: list | None):
    """Try Scripting API first; on 404 fall back to legacy universal/absolute."""
    try:
        return _search_graylog_scripting(base_url, auth, query, from_utc, to_utc, fields)
    except RuntimeError as e:
        if "404" not in str(e):
            raise
    return _search_graylog_legacy(base_url, auth, query, from_utc, to_utc, fields)


def parse_timestamp_to_utc(val) -> datetime | None:
    """Convert Graylog timestamp (string or number) to UTC datetime."""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            pass
    try:
        # Unix seconds or milliseconds
        ts = float(val)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=UTC)
    except Exception:
        pass
    return None


def row_to_message(
    schema_fields: list,
    row: list,
    query: str,
    query_index: int,
    section: str,
    output_fields: frozenset,
) -> dict:
    # Build field values; convert timestamp to Tehran format
    timestamp_tehran = None
    rest = {}
    for i, field_name in enumerate(schema_fields):
        if i >= len(row) or field_name not in output_fields:
            continue
        val = row[i]
        if field_name == "timestamp" and val is not None:
            dt = parse_timestamp_to_utc(val)
            if dt is not None:
                timestamp_tehran = format_timestamp_tehran(dt)
        else:
            rest[field_name] = val
    # Order: timestamp first, then _section, _query, then other fields
    msg = {}
    if timestamp_tehran is not None:
        msg["timestamp"] = timestamp_tehran
    msg["_section"] = section
    msg["_query"] = query
    for k, v in rest.items():
        msg[k] = v
    return msg


def message_contains_filter_keyword(msg: dict, keywords_lower: list[str]) -> bool:
    """True if any value in msg contains any keyword (case-insensitive). Exclude such messages."""
    if not keywords_lower:
        return False
    for key, val in msg.items():
        if key.startswith("_"):
            continue
        if val is None:
            continue
        s = str(val).lower()
        for kw in keywords_lower:
            if kw in s:
                return True
    return False


def _parse_display_timestamp_tehran(s: str) -> datetime | None:
    """Parse our displayed timestamp '2026-02-18 19:46:12 +0330/Tehran' to datetime in Tehran."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    try:
        # Format: 2026-02-18 19:46:12 +0330/Tehran
        dt = datetime.strptime(s.replace(" +0330/Tehran", "").strip(), "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=TEHRAN)
    except ValueError:
        pass
    return None


def aggregate_messages_by_time_frame(
    messages: list[dict], frame_seconds: int, section: str, query: str
) -> list[dict]:
    """Group messages by time buckets of frame_seconds; output one message per bucket with 'message' = all message values joined by newline."""
    if frame_seconds <= 0 or not messages:
        return messages
    buckets: dict[int, list[dict]] = {}
    for m in messages:
        ts_str = m.get("timestamp")
        dt = _parse_display_timestamp_tehran(ts_str) if ts_str else None
        if dt is None:
            continue
        utc_ts = dt.astimezone(UTC).timestamp()
        bucket_id = int(utc_ts // frame_seconds) * frame_seconds
        buckets.setdefault(bucket_id, []).append(m)
    aggregated = []
    for bucket_id in sorted(buckets.keys()):
        bucket_messages = buckets[bucket_id]
        bucket_messages.sort(key=lambda m: m.get("timestamp") or "")
        message_lines = []
        for m in bucket_messages:
            val = m.get("message")
            if val is not None:
                message_lines.append(str(val).strip())
        bucket_start = datetime.fromtimestamp(bucket_id, tz=UTC).astimezone(TEHRAN)
        aggregated.append({
            "timestamp": format_timestamp_tehran(bucket_start),
            "_section": section,
            "_query": query,
            "message": "\n".join(message_lines) if message_lines else "",
        })
    return aggregated


def main():
    parser = argparse.ArgumentParser(
        description="Search Graylog for error logs in a date/time interval (Asia/Tehran)."
    )
    parser.add_argument(
        "--from",
        dest="from_",
        metavar="DATETIME",
        help="Start of interval (overrides .env). E.g. 2024-02-20 10:00. Asia/Tehran.",
    )
    parser.add_argument(
        "--to",
        metavar="DATETIME",
        help="End of interval (overrides .env). E.g. 2024-02-20 18:00. Asia/Tehran.",
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        help="Write JSON to file (default: stdout).",
    )
    parser.add_argument(
        "--no-fields",
        action="store_true",
        help="Omit 'fields' in API request (try to get all message fields).",
    )
    args = parser.parse_args()

    config = load_config()
    if args.from_ is not None or args.to is not None:
        if args.from_ is None or args.to is None:
            raise SystemExit("Provide both --from and --to, or omit both to use .env interval.")
        try:
            from_dt = parse_datetime(args.from_, TEHRAN)
            to_dt = parse_datetime(args.to, TEHRAN)
        except ValueError as e:
            raise SystemExit(e)
    else:
        try:
            from_dt, to_dt = parse_interval_from_env(config)
        except SystemExit:
            raise
    if from_dt >= to_dt:
        raise SystemExit("Start of interval must be before end (check .env or --from/--to)")

    from_utc = from_dt.astimezone(UTC)
    to_utc = to_dt.astimezone(UTC)
    interval = {
        "from_asia_tehran": to_tehran_iso(from_dt),
        "to_asia_tehran": to_tehran_iso(to_dt),
        "from_utc": from_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "to_utc": to_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }

    fields = None if args.no_fields else DEFAULT_FIELDS
    results_by_section = {}
    summary_by_section = {}

    for section_key, queries in config["sections"].items():
        if not queries:
            results_by_section[section_key] = {"queries": [], "results": [], "messages": []}
            summary_by_section[section_key] = {"total_messages": 0, "per_query": []}
            continue
        section_messages = []
        section_results = []
        for idx, query in enumerate(queries):
            try:
                schema_fields, rows = search_graylog(
                    config["base_url"],
                    config["auth"],
                    query,
                    from_utc,
                    to_utc,
                    fields,
                )
            except Exception as e:
                section_results.append({
                    "query_index": idx,
                    "query": query,
                    "error": str(e),
                    "messages": [],
                })
                continue
            messages = [
                row_to_message(schema_fields, row, query, idx, section_key, config["output_fields"])
                for row in rows
            ]
            keywords = config.get("filter_keywords") or []
            messages = [m for m in messages if not message_contains_filter_keyword(m, keywords)]
            if section_key == SECTION_FRONTEND_NEXTJS_PODS and config.get("frontend_nextjs_pods_time_frame_seconds", 0) > 0:
                messages = aggregate_messages_by_time_frame(
                    messages,
                    config["frontend_nextjs_pods_time_frame_seconds"],
                    section_key,
                    query,
                )
            section_messages.extend(messages)
            section_results.append({
                "query_index": idx,
                "query": query,
                "message_count": len(messages),
                "messages": messages,
            })
        results_by_section[section_key] = {
            "queries": queries,
            "results": section_results,
            "messages": section_messages,
        }
        summary_by_section[section_key] = {
            "total_messages": len(section_messages),
            "per_query": [r.get("message_count", 0) for r in section_results],
        }

    out = {
        "interval": interval,
        "sections": {
            section_key: {
                "queries": data["queries"],
                "results": data["results"],
                "messages": data["messages"],
                "summary": summary_by_section[section_key],
            }
            for section_key, data in results_by_section.items()
        },
        "summary": {
            "total_messages": sum(s["total_messages"] for s in summary_by_section.values()),
            "by_section": summary_by_section,
        },
    }

    json_str = json.dumps(out, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_str)
    else:
        print(json_str)


if __name__ == "__main__":
    main()
