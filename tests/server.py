#!/usr/bin/env python3
# File: tests/server.py
#
# Tiny Unix Domain Socket server for feeding ALFS test traces.
#
# Usage:
#   python3 tests/server.py /tmp/event.socket tests/trace1_nice.jsonl
#
# Protocol:
#   - each line in trace file is a full JSON TimeFrame object
#   - send it to client (ALFS) as bytes
#   - then read one response line from client (newline delimited) and print it

import os
import socket
import sys
import time

def main():
    if len(sys.argv) != 3:
        print("Usage: server.py SOCKET_PATH TRACE.jsonl", file=sys.stderr)
        return 2

    path = sys.argv[1]
    trace = sys.argv[2]

    # cleanup old socket
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path)
    srv.listen(1)
    print(f"[server] listening on {path}")

    conn, _ = srv.accept()
    print("[server] client connected")

    with open(trace, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # send JSON (no delimiter required; but we send newline to be nice)
            msg = (line + "\n").encode("utf-8")
            conn.sendall(msg)

            # read one response line
            resp = b""
            while not resp.endswith(b"\n"):
                chunk = conn.recv(4096)
                if not chunk:
                    print("[server] client closed early")
                    return 0
                resp += chunk
            print(resp.decode("utf-8").rstrip())

            # slow down just a bit for readability
            # time.sleep(0.01)

    print("[server] done, closing")
    conn.close()
    srv.close()
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
