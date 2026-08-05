import datetime as dt
import json
import os
import sys
import urllib.parse
import urllib.request


def request_json(url):
    request = urllib.request.Request(url)
    request.add_header("user-agent", "verglas-market-data-example/1")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def iso_time(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def event_range(event):
    data = event.get("data") or {}
    if event["type"] == "org.verglas.http.request":
        raw = bytes(data.get("body", []))
        data = json.loads(raw.decode()) if raw else {}
    start = data.get("start") or data.get("intervalStart")
    end = data.get("end") or data.get("intervalEnd")
    if start and end:
        return iso_time(start), iso_time(end)
    end = dt.datetime.now(dt.timezone.utc)
    return end - dt.timedelta(days=7), end


def quote_row(event, default_symbol):
    data = event.get("data") or {}
    fields = ("timestamp", "open", "high", "low", "close", "volume")
    if not all(field in data for field in fields):
        return None
    return {
        "symbol": data.get("symbol", default_symbol),
        "timestamp": data["timestamp"],
        "open": float(data["open"]),
        "high": float(data["high"]),
        "low": float(data["low"]),
        "close": float(data["close"]),
        "volume": int(data["volume"]),
    }


def yahoo_rows(symbol, start, end):
    query = urllib.parse.urlencode({
        "period1": int(start.timestamp()),
        "period2": int(end.timestamp()),
        "interval": "1d",
        "events": "history",
    })
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{urllib.parse.quote(symbol)}?{query}"
    )
    chart = request_json(url)["chart"]["result"][0]
    quote = chart["indicators"]["quote"][0]
    rows = []
    for index, timestamp in enumerate(chart.get("timestamp", [])):
        values = {name: quote[name][index] for name in
                  ("open", "high", "low", "close", "volume")}
        if any(value is None for value in values.values()):
            continue
        rows.append({
            "symbol": symbol,
            "timestamp": dt.datetime.fromtimestamp(
                timestamp, dt.timezone.utc
            ).isoformat().replace("+00:00", "Z"),
            "open": float(values["open"]),
            "high": float(values["high"]),
            "low": float(values["low"]),
            "close": float(values["close"]),
            "volume": int(values["volume"]),
        })
    return rows


def append_rows(endpoint, table, rows, key):
    body = "".join(json.dumps(row) + "\n" for row in rows).encode()
    url = (
        f"{endpoint}/v1/ingest/{urllib.parse.quote(table)}"
        "?mode=append&format=jsonl"
    )
    request = urllib.request.Request(url, data=body, method="POST")
    request.add_header("content-type", "application/x-ndjson")
    request.add_header("idempotency-key", key)
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def run():
    event = json.loads(os.environ["VERGLAS_CLOUD_EVENT"])
    endpoint = os.environ["VERGLAS_ENDPOINT"].rstrip("/")
    table = os.environ["TARGET"]
    symbol = os.environ.get("SYMBOL", "SPY")
    row = quote_row(event, symbol)
    rows = [row] if row else yahoo_rows(symbol, *event_range(event))
    if not rows:
        return 0
    key = f"{event['source']}:{event['id']}"
    return int(append_rows(endpoint, table, rows, key)["rowsCommitted"])


def main():
    try:
        result = {"rows": run(), "error": None}
        status = 0
    except Exception as error:
        result = {"rows": 0, "error": str(error)}
        status = 1
    with open(os.environ["RESULT_PATH"], "w", encoding="utf-8") as output:
        json.dump(result, output)
    return status


if __name__ == "__main__":
    sys.exit(main())
