"""Ingest SPY OHLCV rows from Yahoo Finance or a quote CloudEvent."""

import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request


def request_json(method, url, body=None, headers=None):
    """Send one JSON request and return the decoded response."""
    encoded = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=encoded, method=method)
    request.add_header("user-agent", "verglas-spy-example/1")
    if encoded is not None:
        request.add_header("content-type", "application/json")
    for name, value in (headers or {}).items():
        request.add_header(name, value)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def append_jsonl(url, rows, key):
    """Append one JSON Lines batch with the CloudEvent identity."""
    body = "".join(json.dumps(row) + "\n" for row in rows).encode()
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("content-type", "application/x-ndjson")
    request.add_header("idempotency-key", key)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def iso_time(value):
    """Parse an ISO date or timestamp as a UTC datetime."""
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def range_from_event(event):
    """Resolve the Yahoo request interval from one Verglas CloudEvent."""
    data = event.get("data") or {}
    if event["type"] == "org.verglas.http.request":
        raw = bytes(data.get("body", []))
        data = json.loads(raw.decode()) if raw else {}
    start = data.get("start") or data.get("intervalStart")
    end = data.get("end") or data.get("intervalEnd")
    if start and end:
        return iso_time(start), iso_time(end)
    end_time = dt.datetime.now(dt.timezone.utc)
    return end_time - dt.timedelta(days=7), end_time


def quote_row(event, symbol):
    """Convert a broker quote CloudEvent into one OHLCV row."""
    data = event.get("data") or {}
    required = ("timestamp", "open", "high", "low", "close", "volume")
    if not all(field in data for field in required):
        return None
    return {
        "symbol": data.get("symbol", symbol),
        "timestamp": data["timestamp"],
        "open": float(data["open"]),
        "high": float(data["high"]),
        "low": float(data["low"]),
        "close": float(data["close"]),
        "volume": int(data["volume"]),
    }


def yahoo_rows(symbol, start, end):
    """Fetch daily OHLCV rows for the half-open logical interval."""
    query = urllib.parse.urlencode(
        {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": "1d",
            "events": "history",
        }
    )
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{urllib.parse.quote(symbol)}?{query}"
    )
    chart = request_json("GET", url)["chart"]["result"][0]
    quote = chart["indicators"]["quote"][0]
    rows = []
    for index, timestamp in enumerate(chart.get("timestamp", [])):
        values = {
            name: quote[name][index]
            for name in ("open", "high", "low", "close", "volume")
        }
        if any(value is None for value in values.values()):
            continue
        rows.append(
            {
                "symbol": symbol,
                "timestamp": dt.datetime.fromtimestamp(
                    timestamp, dt.timezone.utc
                ).isoformat().replace("+00:00", "Z"),
                "open": float(values["open"]),
                "high": float(values["high"]),
                "low": float(values["low"]),
                "close": float(values["close"]),
                "volume": int(values["volume"]),
            }
        )
    return rows


def run():
    """Resolve the trigger, ingest rows, and commit them idempotently."""
    event = json.loads(os.environ["VERGLAS_CLOUD_EVENT"])
    endpoint = os.environ["VERGLAS_ENDPOINT"].rstrip("/")
    target = os.environ["TARGET"]
    symbol = os.environ.get("SYMBOL", "SPY")

    row = quote_row(event, symbol)
    if row is not None:
        rows = [row]
    else:
        start, end = range_from_event(event)
        rows = yahoo_rows(symbol, start, end)
    if not rows:
        return 0

    key = f"{event['source']}:{event['id']}"
    response = append_jsonl(
        f"{endpoint}/v1/ingest/{urllib.parse.quote(target)}"
        "?mode=append&format=jsonl",
        rows,
        key,
    )
    return int(response["rowsCommitted"])


def main():
    """Write the subprocess result contract even when ingestion fails."""
    result_path = os.environ["RESULT_PATH"]
    try:
        result = {"rows": run(), "error": None}
        status = 0
    except Exception as error:  # The result file is the worker error boundary.
        result = {"rows": 0, "error": str(error)}
        status = 1
    with open(result_path, "w", encoding="utf-8") as output:
        json.dump(result, output)
    return status


if __name__ == "__main__":
    sys.exit(main())
