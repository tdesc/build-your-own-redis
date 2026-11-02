"""Self-written test runner for mini-redis (raw sockets, no dependencies).

Usage: python3 tests/test_stages.py [port]
Starts nothing itself — the server must be running on the given port.
"""

import socket
import sys
import time


class RespClient:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)

    def send_raw(self, data: bytes):
        self.sock.sendall(data)

    def command(self, *args) -> bytes:
        out = f"*{len(args)}\r\n".encode()
        for a in args:
            a = a.encode() if isinstance(a, str) else a
            out += b"$" + str(len(a)).encode() + b"\r\n" + a + b"\r\n"
        self.send_raw(out)
        return self.read_reply()

    def read_reply(self) -> bytes:
        """Read one complete RESP reply (minimal parser for tests)."""
        buf = b""
        while True:
            while b"\r\n" not in buf:
                buf += self.sock.recv(4096)
            kind, rest = buf[:1], buf[1:]
            if kind in (b"+", b"-", b":"):
                line, buf = rest.split(b"\r\n", 1)
                return kind + line
            if kind == b"$":
                line, buf = rest.split(b"\r\n", 1)
                length = int(line)
                if length == -1:
                    return b"$-1"
                while len(buf) < length + 2:
                    buf += self.sock.recv(4096)
                payload, buf = buf[:length], buf[length + 2:]
                return b"$" + line + b"\r\n" + payload
            if kind == b"*":
                line, buf = rest.split(b"\r\n", 1)
                count = int(line)
                parts = []
                for _ in range(count):
                    # recursively read one bulk per element
                    self.sock.settimeout(5)
                    reply = self._read_nested(buf)
                    if isinstance(reply, tuple):
                        parts.append(reply[0])
                        buf = reply[1]
                    else:
                        parts.append(reply)
                        buf = b""
                return b"*" + str(count).encode() + b"\r\n" + b"".join(
                    p if p.startswith(b"$") else p for p in parts
                )
            raise AssertionError(f"unknown reply prefix: {buf[:20]!r}")

    def _read_nested(self, buf):
        """Read one bulk string that may already be in buf or arrive later."""
        while b"\r\n" not in buf:
            buf += self.sock.recv(4096)
        line, buf = buf.split(b"\r\n", 1)
        length = int(line[1:])
        if length == -1:
            return b"$-1", buf
        while len(buf) < length + 2:
            buf += self.sock.recv(4096)
        return b"$" + line + b"\r\n" + buf[:length], buf[length + 2:]

    def close(self):
        self.sock.close()


PASS = "\033[32m✔\033[0m"
FAIL = "\033[31m✘\033[0m"
results = []


def check(name, actual, expected):
    ok = actual == expected
    results.append(ok)
    mark = PASS if ok else FAIL
    print(f"{mark} {name}")
    if not ok:
        print(f"    expected: {expected!r}")
        print(f"    actual:   {actual!r}")


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6379

    # Stage 2: PING (RESP array form)
    c1 = RespClient(port)
    check("stage2: PING -> +PONG", c1.command("PING"), b"+PONG")
    check("stage2: PING hi -> bulk 'hi'", c1.command("PING", "hi"), b"$2\r\nhi")

    # Inline form
    c1.send_raw(b"PING\r\n")
    check("stage2: inline PING -> +PONG", c1.read_reply(), b"+PONG")

    # Stage 4: ECHO
    check("stage4: ECHO hey", c1.command("ECHO", "hey"), b"$3\r\nhey")

    # Stage 5: SET / GET
    check("stage5: SET k v -> +OK", c1.command("SET", "k", "v"), b"+OK")
    check("stage5: GET k -> v", c1.command("GET", "k"), b"$1\r\nv")
    check("stage5: GET missing -> nil", c1.command("GET", "nope"), b"$-1")

    # Stage 3: concurrent clients — client B sees client A's write
    c2 = RespClient(port)
    check("stage3: client B reads A's key", c2.command("GET", "k"), b"$1\r\nv")
    c2.command("SET", "fromB", "yes")
    check("stage3: client A reads B's key", c1.command("GET", "fromB"), b"$3\r\nyes")
    check("stage3: client B PING", c2.command("PING"), b"+PONG")

    # Stage 6: expiry (PX)
    c1.command("SET", "ttl", "gone-soon", "PX", "300")
    check("stage6: GET before expiry", c1.command("GET", "ttl"), b"$9\r\ngone-soon")
    time.sleep(0.45)
    check("stage6: GET after expiry -> nil", c1.command("GET", "ttl"), b"$-1")

    # Stage 7: streams
    def bulk_payload(reply: bytes):
        if reply == b"$-1":
            return None
        line_end = reply.index(b"\r\n")
        return reply[line_end + 2:]

    eid1 = bulk_payload(c1.command("XADD", "mystream", "*", "field1", "value1"))
    eid2 = bulk_payload(c1.command("XADD", "mystream", "*", "field2", "value2"))
    ok_ids = eid1 is not None and b"-" in eid1 and eid2 > eid1
    check("stage7: XADD returns ms-seq ids (ascending)", ok_ids, True)
    rng = c1.command("XRANGE", "mystream", "-", "+")
    has_both = b"field1" in rng and b"value2" in rng and eid1 in rng and eid2 in rng
    check("stage7: XRANGE - + returns both entries in order", has_both, True)
    rng1 = c1.command("XRANGE", "mystream", eid1.decode(), eid1.decode())
    check("stage7: XRANGE by exact id", b"field1" in rng1 and b"field2" not in rng1, True)

    # Unknown command
    check("misc: unknown command -> -ERR", c1.command("FROBNICATE"), b"-ERR unknown command 'FROBNICATE'")

    c1.close()
    c2.close()

    passed = sum(results)
    print(f"\n{passed}/{len(results)} checks passed")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
