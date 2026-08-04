#!/usr/bin/env python3
"""Structural proof that a `/v1/query`-shaped endpoint streams as multiple
HTTP chunks, not one buffered write. Opens a raw socket, sends the POST by
hand, and parses the raw HTTP/1.1 chunked-transfer-encoding wire format
itself (chunk-size line, CRLF, chunk bytes, CRLF, repeat, terminated by a
0-size chunk) rather than trusting an HTTP client library to hide the
framing. Reports every chunk's size, and reassembles + parses the body to
prove it is still exact JSON with the expected row count.

Usage: chunk_probe.py <port> <sql> [path]
"""
import json
import socket
import sys
import time

HOST = "127.0.0.1"
PORT = int(sys.argv[1])
SQL = sys.argv[2]
PATH = sys.argv[3] if len(sys.argv) > 3 else "/v1/query"

body = json.dumps({"sql": SQL}).encode()
req = (
    f"POST {PATH} HTTP/1.1\r\n"
    f"Host: {HOST}:{PORT}\r\n"
    f"Content-Type: application/json\r\n"
    f"Content-Length: {len(body)}\r\n"
    f"Connection: close\r\n"
    f"\r\n"
).encode() + body

s = socket.create_connection((HOST, PORT), timeout=30)
s.sendall(req)

buf = b""


def recv_more():
    global buf
    chunk = s.recv(65536)
    if not chunk:
        raise EOFError("connection closed before terminal chunk")
    buf += chunk


def read_line():
    global buf
    while b"\r\n" not in buf:
        recv_more()
    line, buf = buf.split(b"\r\n", 1)
    return line


def read_exact(n):
    global buf
    while len(buf) < n:
        recv_more()
    data, buf = buf[:n], buf[n:]
    return data


status_line = read_line().decode()
headers = {}
while True:
    line = read_line()
    if line == b"":
        break
    k, _, v = line.decode().partition(":")
    headers[k.strip().lower()] = v.strip()

transfer_encoding = headers.get("transfer-encoding", "")
print(f"status: {status_line}")
print(f"transfer-encoding: {transfer_encoding!r}")

if "chunked" not in transfer_encoding.lower():
    print("NOT CHUNKED — cannot prove streamed arrival structurally")
    sys.exit(1)

chunks = []
body_out = bytearray()
while True:
    size_line = read_line()
    size = int(size_line.split(b";")[0].strip(), 16)
    if size == 0:
        while True:
            if read_line() == b"":
                break
        break
    data = read_exact(size)
    read_exact(2)
    chunks.append(size)
    body_out += data
s.close()

total = sum(chunks)
print(f"HTTP chunk count: {len(chunks)}  total_bytes: {total}  largest_chunk: {max(chunks)} ({100*max(chunks)/total:.2f}% of body)")

parsed = json.loads(bytes(body_out))
row_count_ok = len(parsed["rows"]) == parsed["row_count"]
print(f"parsed JSON OK: row_count={parsed['row_count']} len(rows)==row_count: {row_count_ok}")

multi_chunk = len(chunks) > 1
no_single_dominant_chunk = max(chunks) < 0.5 * total
if multi_chunk and no_single_dominant_chunk and row_count_ok:
    print("STRUCTURAL PROOF: response arrived as many genuine chunks, not one buffered write.")
else:
    print("NOT PROVEN")
    sys.exit(1)
