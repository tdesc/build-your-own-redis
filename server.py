"""A miniature Redis server in pure Python (no dependencies).

Built as a hands-on follow-up to the nowinterview Redis labs, following the
stage structure popularized by CodeCrafters and the public guide
"Write your own miniature Redis with Python" (charlesleifer.com) listed in
codecrafters-io/build-your-own-x.

Stages:
  1. Bind to a port and accept connections.
  2. Respond to PING (inline and RESP array forms).
  3. Handle concurrent clients (thread per connection).
  4. ECHO.
  5. SET / GET (bulk strings, nil for missing keys).
  6. Key expiry (SET ... PX <ms>, passive expiry on GET).
  7. Streams: XADD (auto ids ms-seq) and XRANGE (- +).

Protocol references: https://redis.io/docs/latest/develop/reference/protocol-spec
"""

import argparse
import socket
import sys
import threading
import time


class Store:
    """Thread-safe key-value store with per-key expiry and streams."""

    def __init__(self):
        self._lock = threading.Lock()
        self._kv = {}       # key -> (value: str, expires_at: float | None)
        self._streams = {}  # key -> list[(id: str, fields: list[str])]

    # -- keys ---------------------------------------------------------------
    def set(self, key, value, px_ms=None):
        expires_at = time.monotonic() + px_ms / 1000.0 if px_ms else None
        with self._lock:
            self._kv[key] = (value, expires_at)

    def get(self, key):
        with self._lock:
            item = self._kv.get(key)
            if item is None:
                return None
            value, expires_at = item
            if expires_at is not None and time.monotonic() >= expires_at:
                del self._kv[key]  # passive expiry
                return None
            return value

    # -- streams --------------------------------------------------------------
    def xadd(self, key, fields):
        """Append to a stream with an auto id '<ms>-<seq>'; returns the id."""
        with self._lock:
            entries = self._streams.setdefault(key, [])
            ms = int(time.time() * 1000)
            last_ms, last_seq = (0, 0)
            if entries:
                last_ms, last_seq = map(int, entries[-1][0].split("-"))
            if ms < last_ms:
                ms = last_ms
            seq = last_seq + 1 if ms == last_ms else 0
            entry_id = f"{ms}-{seq}"
            entries.append((entry_id, list(fields)))
            return entry_id

    def xrange(self, key, start="-", end="+"):
        with self._lock:
            entries = list(self._streams.get(key, []))
        out = []
        for entry_id, fields in entries:
            entry_ms, entry_seq = map(int, entry_id.split("-"))
            if start not in ("-",) and entry_tuple(entry_id) < entry_tuple(start):
                continue
            if end not in ("+",) and entry_tuple(entry_id) > entry_tuple(end):
                continue
            out.append((entry_id, fields))
        return out


def entry_tuple(entry_id):
    ms, seq = entry_id.split("-")
    return (int(ms), int(seq))


# -- RESP encoding ---------------------------------------------------------------

def simple(value: str) -> bytes:
    return f"+{value}\r\n".encode()


def error(value: str) -> bytes:
    return f"-{value}\r\n".encode()


def bulk(value) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if not isinstance(value, bytes):
        value = str(value).encode()
    return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"


def array(items) -> bytes:
    out = b"*" + str(len(items)).encode() + b"\r\n"
    return out + b"".join(items)


def encode_response(value):
    """Encode a python value the way redis would reply."""
    if isinstance(value, SimpleString):
        return simple(value.value)
    if isinstance(value, RESPError):
        return error(value.value)
    if isinstance(value, list):
        return array([bulk(v) for v in value])  # flat: entries as [id, f1, v1, ...]
    return bulk(value)


class SimpleString:
    """Marker for +OK style replies (distinct from a bulk string)."""

    def __init__(self, value):
        self.value = value


class RESPError:
    """Marker for -ERR style replies."""

    def __init__(self, value):
        self.value = value


# -- RESP parsing -----------------------------------------------------------------

def parse_command(buffer: bytes):
    """Parse one command from the buffer.

    Returns (command_args | None, rest_of_buffer). command_args is a list of
    byte strings, e.g. [b"PING"] or [b"SET", b"key", b"value"], or None when
    more data is needed.
    """
    if not buffer:
        return None, buffer
    # RESP array form: *<n>\r\n $<len>\r\n <data>\r\n ...
    if buffer[:1] == b"*":
        pos = buffer.find(b"\r\n")
        if pos == -1:
            return None, buffer
        try:
            count = int(buffer[1:pos])
        except ValueError:
            return None, buffer  # wait for more data; caller guards loops
        pos += 2
        args = []
        for _ in range(count):
            if buffer[pos:pos + 1] != b"$":
                return None, buffer
            lend = buffer.find(b"\r\n", pos)
            if lend == -1:
                return None, buffer
            length = int(buffer[pos + 1:lend])
            start = lend + 2
            end = start + length
            if len(buffer) < end + 2:
                return None, buffer
            args.append(buffer[start:end])
            pos = end + 2
        return args, buffer[pos:]
    # Inline form: PING\r\n
    pos = buffer.find(b"\r\n")
    if pos == -1:
        return None, buffer
    line = buffer[:pos].strip()
    if not line:
        return [], buffer[pos + 2:]
    return line.split(), buffer[pos + 2:]


# -- Command handling ---------------------------------------------------------------

def handle_command(args, store: Store):
    """Dispatch one command; returns a python value for encode_response()."""
    if not args:
        return None
    cmd = args[0].decode().upper()
    arity = len(args) - 1

    if cmd == "PING":
        if arity == 0:
            return SimpleString("PONG")
        if arity == 1:
            return args[1].decode()  # PING <message> echoes as bulk string
        return RESPError("ERR wrong number of arguments for 'ping' command")

    if cmd == "ECHO":
        if arity != 1:
            return RESPError("ERR wrong number of arguments for 'echo' command")
        return args[1].decode()

    if cmd == "SET":
        if arity < 2:
            return RESPError("ERR wrong number of arguments for 'set' command")
        key, value = args[1].decode(), args[2].decode()
        px_ms = None
        i = 3
        while i < arity:
            opt = args[i].decode().upper()
            if opt == "PX" and i + 1 <= arity:
                try:
                    px_ms = int(args[i + 1])
                except ValueError:
                    return RESPError("ERR value is not an integer or out of range")
                i += 2
                continue
            return RESPError("ERR syntax error")
        store.set(key, value, px_ms)
        return SimpleString("OK")

    if cmd == "GET":
        if arity != 1:
            return RESPError("ERR wrong number of arguments for 'get' command")
        return store.get(args[1].decode())

    if cmd == "XADD":
        # XADD key ID field value [field value ...]
        # tokens after the command: key, id, then field/value pairs.
        if arity < 4 or (arity - 2) % 2 != 0:
            return RESPError("ERR wrong number of arguments for 'xadd' command")
        key = args[1].decode()
        entry_id = args[2].decode()
        if entry_id != "*":
            return RESPError("ERR only auto ids (*) are supported")
        fields = [a.decode() for a in args[3:]]
        return store.xadd(key, fields)

    if cmd == "XRANGE":
        # XRANGE key start end
        if arity != 3:
            return RESPError("ERR wrong number of arguments for 'xrange' command")
        key = args[1].decode()
        start, end = args[2].decode(), args[3].decode()
        entries = store.xrange(key, start, end)
        flat = []
        for entry_id, fields in entries:
            flat.append(entry_id)
            flat.extend(fields)
        return flat

    if cmd in ("CONFIG", "INFO", "FLUSHALL"):
        # Minimal politeness so redis-cli connects cleanly.
        if cmd == "CONFIG" and arity >= 1:
            return array([])
        if cmd == "INFO":
            return bulk("# mini-redis\r\n")
        if cmd == "FLUSHALL":
            return SimpleString("OK")

    return RESPError(f"ERR unknown command '{cmd}'")


# -- Server ---------------------------------------------------------------------------

def handle_connection(conn: socket.socket, addr, store: Store):
    print(f"client connected: {addr}", file=sys.stderr)
    buffer = b""
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buffer += data
            while True:
                args, buffer = parse_command(buffer)
                if args is None:
                    break
                reply = encode_response(handle_command(args, store))
                conn.sendall(reply)
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        conn.close()
        print(f"client disconnected: {addr}", file=sys.stderr)


def serve(port: int):
    store = Store()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen()
    print(f"mini-redis listening on 127.0.0.1:{port}", file=sys.stderr)
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_connection, args=(conn, addr, store), daemon=True).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="mini-redis")
    parser.add_argument("--port", type=int, default=6379)
    serve(parser.parse_args().port)
