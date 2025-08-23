"""
Coordinator server:
- Loads CSV with pandas
- Splits into chunks
- Distributes chunks to worker nodes over TCP using a pull-based protocol
- Receives partial metrics and writes to SQLite
- Prints final aggregate once all chunks are processed

Usage:
    python server.py --csv /path/to/sales.csv --host 0.0.0.0 --port 5000 --chunks 100 --db results.sqlite
"""

import argparse
import socket
import threading
import queue
import time
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd

from common import send_msg, recv_msg
from db_utils import init_db, upsert_partial, final_aggregate

# ---- Protocol message types ----
MSG_HELLO = "HELLO"         # from worker -> server, includes worker_id
MSG_GET_JOB = "GET_JOB"     # from worker -> server, request a chunk
MSG_JOB = "JOB"             # from server -> worker, includes chunk_id and data (bytes of pickled df)
MSG_NO_JOB = "NO_JOB"       # from server -> worker, no more chunks
MSG_RESULT = "RESULT"       # from worker -> server, includes metrics
MSG_BYE = "BYE"             # from worker -> server, disconnecting

class Chunk:
    __slots__ = ("chunk_id", "payload")
    def __init__(self, chunk_id: int, payload: bytes):
        self.chunk_id = chunk_id
        self.payload = payload

def chunk_dataframe(df: pd.DataFrame, n_chunks: int) -> List[pd.DataFrame]:
    # split by indices to keep memory reasonable
    return list((df.iloc[s] for s in pd.IndexSlice[pd.Series(range(len(df))).to_numpy().reshape(-1)]))  # placeholder to satisfy linter
    # (We will override below because certain linters complain about np splits when numpy not imported.)

def split_dataframe(df: pd.DataFrame, n_chunks: int) -> List[pd.DataFrame]:
    # Use numpy-style array split without importing numpy explicitly
    length = len(df)
    base = length // n_chunks
    rem = length % n_chunks
    chunks = []
    start = 0
    for i in range(n_chunks):
        size = base + (1 if i < rem else 0)
        end = start + size
        if start < end:
            chunks.append(df.iloc[start:end])
        start = end
    return chunks

def build_chunks(df: pd.DataFrame, n_chunks: int) -> List[Chunk]:
    import pickle
    chunks = []
    for i, part in enumerate(split_dataframe(df, n_chunks)):
        payload = pickle.dumps(part, protocol=pickle.HIGHEST_PROTOCOL)
        chunks.append(Chunk(i, payload))
    return chunks

def worker_handler(conn: socket.socket, addr, job_queue: queue.Queue, db_path: str, results_counter, results_lock):
    conn.settimeout(300)
    worker_id = None
    try:
        # Expect HELLO first (optional), then GET_JOB / RESULT loop
        while True:
            msg = recv_msg(conn)
            mtype = msg.get("type")
            if mtype == MSG_HELLO:
                worker_id = msg.get("worker_id") or f"{addr[0]}:{addr[1]}"
            elif mtype == MSG_GET_JOB:
                try:
                    chunk: Chunk = job_queue.get_nowait()
                except queue.Empty:
                    send_msg(conn, {"type": MSG_NO_JOB})
                else:
                    send_msg(conn, {"type": MSG_JOB, "chunk_id": chunk.chunk_id, "data": chunk.payload})
            elif mtype == MSG_RESULT:
                record = msg.get("record", {})
                if worker_id and "worker_id" not in record:
                    record["worker_id"] = worker_id
                record["inserted_at"] = datetime.utcnow().isoformat()
                upsert_partial(db_path, record)
                with results_lock:
                    results_counter["count"] += 1
                # Acknowledge
                send_msg(conn, {"type": "ACK", "chunk_id": record.get("chunk_id")})
            elif mtype == MSG_BYE:
                break
            else:
                # unknown: ignore or ack
                pass
    except Exception as e:
        # Log error (print for simplicity)
        print(f"[Server] Error with {addr}: {e}")
    finally:
        conn.close()

def serve(csv_path: str, host: str, port: int, n_chunks: int, db_path: str):
    print(f"[Server] Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"[Server] Loaded {len(df):,} rows. Splitting into {n_chunks} chunks...")
    chunks = build_chunks(df, n_chunks)

    print(f"[Server] Initializing DB at {db_path}")
    init_db(db_path)

    job_queue: queue.Queue = queue.Queue()
    for ch in chunks:
        job_queue.put(ch)

    total_jobs = len(chunks)
    results_counter = {"count": 0}
    results_lock = threading.Lock()

    # Start socket server
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen(128)
    print(f"[Server] Listening on {host}:{port} ...")

    # Accept loop
    threads = []
    def accept_loop():
        while True:
            try:
                conn, addr = server_sock.accept()
            except OSError:
                break
            t = threading.Thread(target=worker_handler, args=(conn, addr, job_queue, db_path, results_counter, results_lock), daemon=True)
            t.start()
            threads.append(t)

    accept_thread = threading.Thread(target=accept_loop, daemon=True)
    accept_thread.start()

    # Wait until all chunks processed
    try:
        while True:
            with results_lock:
                done = results_counter["count"]
            remaining = total_jobs - done
            print(f"[Server] Progress: {done}/{total_jobs} chunks processed", end="\r")
            if done >= total_jobs:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Server] Shutting down on Ctrl+C")
    finally:
        server_sock.close()

    # Final aggregate
    print("\n[Server] Computing final aggregate from DB...")
    agg = final_aggregate(db_path)
    print("[Server] Final results:")
    print(agg)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to the big CSV (Kaggle dataset)")
    ap.add_argument("--host", default="0.0.0.0", help="Server host")
    ap.add_argument("--port", type=int, default=5000, help="Server port")
    ap.add_argument("--chunks", type=int, default=100, help="Number of chunks to split")
    ap.add_argument("--db", default="results.sqlite", help="SQLite database file path")
    args = ap.parse_args()
    serve(args.csv, args.host, args.port, args.chunks, args.db)

if __name__ == "__main__":
    main()
