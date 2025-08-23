"""
Worker node:
- Connects to server, identifies with a worker_id
- Repeatedly requests jobs (dataframe chunks), processes them, sends partial metrics
- Exits when server sends NO_JOB

Usage:
    python worker.py --server 127.0.0.1 --port 5000 --worker-id w1
"""

import argparse
import socket
from typing import Dict, Any

import pandas as pd
import pickle

from common import send_msg, recv_msg

# ---- Protocol message types ----
MSG_HELLO = "HELLO"
MSG_GET_JOB = "GET_JOB"
MSG_JOB = "JOB"
MSG_NO_JOB = "NO_JOB"
MSG_RESULT = "RESULT"
MSG_BYE = "BYE"

def compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    # Try to robustly find price and quantity columns
    cols = {c.lower(): c for c in df.columns}
    price_col = None
    for cand in ["price", "unitprice", "unit_price"]:
        if cand in cols:
            price_col = cols[cand]
            break
    if price_col is None:
        raise ValueError(f"Could not find a price column among: {list(df.columns)}")

    qty_col = None
    for cand in ["quantity", "qty", "units"]:
        if cand in cols:
            qty_col = cols[cand]
            break
    if qty_col is None:
        raise ValueError(f"Could not find a quantity column among: {list(df.columns)}")

    prices = pd.to_numeric(df[price_col], errors="coerce")
    qty = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    # Rows considered: where price is not NaN
    valid = prices.notna()
    prices = prices[valid]
    qty = qty[valid]

    rows_processed = int(valid.sum())
    total_sales = float((prices * qty).sum())

    min_price = float(prices.min()) if rows_processed else 0.0
    max_price = float(prices.max()) if rows_processed else 0.0
    avg_price = float(prices.mean()) if rows_processed else 0.0

    return {
        "rows_processed": rows_processed,
        "total_sales": total_sales,
        "min_price": min_price,
        "max_price": max_price,
        "avg_price": avg_price,
    }

def run_worker(server_host: str, server_port: int, worker_id: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_host, server_port))

    # Introduce ourselves
    send_msg(sock, {"type": MSG_HELLO, "worker_id": worker_id})

    try:
        while True:
            # Ask for a job
            send_msg(sock, {"type": MSG_GET_JOB})
            msg = recv_msg(sock)
            mtype = msg.get("type")
            if mtype == MSG_NO_JOB:
                # Done
                break
            elif mtype == MSG_JOB:
                chunk_id = msg["chunk_id"]
                df = pickle.loads(msg["data"])
                metrics = compute_metrics(df)
                record = {"worker_id": worker_id, "chunk_id": chunk_id, **metrics}
                send_msg(sock, {"type": MSG_RESULT, "record": record})
                # Wait for ACK (optional)
                ack = recv_msg(sock)
            else:
                # Unexpected message; continue
                continue
    finally:
        try:
            send_msg(sock, {"type": MSG_BYE})
        except Exception:
            pass
        sock.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True, help="Server host")
    ap.add_argument("--port", type=int, default=5000, help="Server port")
    ap.add_argument("--worker-id", default=None, help="Worker identifier string")
    args = ap.parse_args()

    worker_id = args.worker_id or socket.gethostname()
    run_worker(args.server, args.port, worker_id)

if __name__ == "__main__":
    main()
