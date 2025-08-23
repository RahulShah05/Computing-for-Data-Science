"""
Common utilities for socket framing and simple message protocol using pickle.
Messages are Python dicts with at least a "type" field.
Framing: 4-byte big-endian length prefix, followed by pickled payload.
"""

import pickle
import socket
import struct
from typing import Dict, Any

LENGTH_PREFIX_FMT = "!I"  # 4 bytes, big-endian unsigned int

def send_msg(sock: socket.socket, msg: Dict[str, Any]) -> None:
    """Serialize and send a dict message with length prefix."""
    payload = pickle.dumps(msg, protocol=pickle.HIGHEST_PROTOCOL)
    header = struct.pack(LENGTH_PREFIX_FMT, len(payload))
    sock.sendall(header + payload)

def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes or raise ConnectionError."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Socket closed during recv_exact")
        buf.extend(chunk)
    return bytes(buf)

def recv_msg(sock: socket.socket) -> Dict[str, Any]:
    """Receive a dict message with length prefix and unpickle it."""
    header = recv_exact(sock, struct.calcsize(LENGTH_PREFIX_FMT))
    (length,) = struct.unpack(LENGTH_PREFIX_FMT, header)
    payload = recv_exact(sock, length)
    return pickle.loads(payload)
