# Distributed Sales Analysis (Sockets + SQLite + Pandas)

This implementation coordinates multiple Python worker processes to analyze a large CSV (e.g., the Kaggle "Sample Sales Data (5 million transactions)") in parallel. Workers pull DataFrame chunks over TCP, compute partial metrics, and the server writes results to SQLite for final aggregation.

## Features
- Pull-based job distribution over raw TCP sockets
- Length-prefixed, pickle-serialized messages
- Fault-tolerant upserts of partial results (idempotent per worker+chunk)
- Threaded server handling many workers concurrently
- Final aggregation done in SQL (weighted mean of per-chunk averages)

## Data expectations
Your CSV should include (case-insensitive) columns for **Price** and **Quantity**. Common alternative names are supported:
- `Price` / `UnitPrice` / `Unit_Price`
- `Quantity` / `Qty` / `Units`

## Quick start

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start the server**
   ```bash
   python server.py --csv /path/to/sales_5m.csv --host 0.0.0.0 --port 5000 --chunks 200 --db results.sqlite
   ```

3. **Start workers (in separate terminals / machines)**
   ```bash
   python worker.py --server <SERVER_IP> --port 5000 --worker-id worker-1
   python worker.py --server <SERVER_IP> --port 5000 --worker-id worker-2
   # ... start more as needed
   ```

4. **Watch server logs**
   - Server prints progress as chunks are completed.
   - When all chunks are done, it prints the **final aggregate** from SQLite:
     ```
     {'total_rows': ..., 'total_sales': ..., 'min_price': ..., 'max_price': ..., 'avg_price': ...}
     ```

## Design notes

- **Chunking**: The server loads the CSV (using `pandas.read_csv`) and splits it into `--chunks` roughly equal parts. Each chunk is pickled and queued.
- **Protocol**:
  - Worker connects → sends `HELLO`.
  - Worker repeatedly sends `GET_JOB`.
  - Server replies `JOB` with `chunk_id` and pickled DataFrame or `NO_JOB` when queue empty.
  - Worker computes metrics and sends `RESULT` with `{worker_id, chunk_id, rows_processed, total_sales, min_price, max_price, avg_price}`.
  - Server upserts into `worker_results` (primary key: `(worker_id, chunk_id)`), so retries are safe.
- **DB**: SQLite with WAL mode. Final aggregation uses SQL, including a weighted average of per-chunk means.
- **Scaling**: For very large datasets or many workers, consider:
  - Streaming CSV in batches instead of loading entire DataFrame.
  - Compressing payloads before sending (e.g., `zlib`).
  - Switching to Postgres/MySQL and using async IO.

## Switching databases
Replace the implementations in `db_utils.py` with a Postgres/MySQL connector and update the SQL syntax for `UPSERT`. The rest of the code stays the same.

## Troubleshooting
- **ValueError: Could not find price/quantity column**: Ensure your CSV has compatible column names or edit `compute_metrics()` in `worker.py`.
- **Firewall / ports**: Make sure the server port is reachable from worker machines.
- **Memory**: Loading 5M rows might be heavy; adjust `--chunks` to balance memory and network overhead.
- **Throughput tips**: Increase workers, run server where CSV resides, consider enabling compression.

## Security note
This demo uses raw sockets and pickle. Do **not** expose it to untrusted networks. For production, use TLS + a safer serialization format (e.g., JSON) and sign messages.
