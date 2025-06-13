"""
Partition a large ERCOT SQLite database into time-based chunks (e.g., daily, monthly, yearly).

Usage:
    python scripts/partition_ercot_db.py --db <input_db> --outdir <output_dir> --mode <1d|1M|6M|1Y>

- --db: Path to the source SQLite database.
- --outdir: Directory to write partitioned DBs.
- --mode: Partitioning mode: 1d (day), 1M (month), 6M (6 months), 1Y (year).

This script will create a new SQLite file for each partition, containing all relevant tables and only the data for that period.
"""
import os
import sqlite3
import argparse
from datetime import datetime, timedelta

# Tables and their date columns
TABLES = {
    "SETTLEMENT_POINT_PRICES": ("DeliveryDate",),
    "BIDS": ("DeliveryDate",),
    "BID_AWARDS": ("DeliveryDate",),
    "OFFERS": ("DeliveryDate",),
    "OFFER_AWARDS": ("DeliveryDate",),
    "FINAL": ("deliveryDate",),  # lowercase in FINAL
}

DATE_FORMAT = "%Y-%m-%d"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Partition ERCOT SQLite DB by time period.")
    parser.add_argument("--db", required=True, help="Path to input SQLite DB.")
    parser.add_argument("--outdir", required=True,
                        help="Output directory for partitioned DBs.")
    parser.add_argument("--mode", required=True, choices=[
                        "1d", "1M", "6M", "1Y"], help="Partition mode: 1d=day, 1M=month, 6M=6 months, 1Y=year.")
    return parser.parse_args()


def get_date_ranges(conn, mode, table, date_col):
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT {date_col} FROM {table}")
    dates = sorted(set(row[0] for row in cur.fetchall() if row[0]))
    if not dates:
        return []
    # Normalize to datetime, try multiple formats

    def parse_date(d):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%b-%Y"):
            try:
                return datetime.strptime(d, fmt)
            except Exception:
                continue
        raise ValueError(f"Unrecognized date format: {d}")
    date_objs = [parse_date(d) for d in dates]
    min_date, max_date = min(date_objs), max(date_objs)
    ranges = []
    if mode == "1d":
        d = min_date
        while d <= max_date:
            ranges.append((d, d))
            d += timedelta(days=1)
    elif mode == "1M":
        d = min_date.replace(day=1)
        while d <= max_date:
            end = (d.replace(day=28) + timedelta(days=4)
                   ).replace(day=1) - timedelta(days=1)
            end = min(end, max_date)
            ranges.append((d, end))
            d = (end + timedelta(days=1)).replace(day=1)
    elif mode == "6M":
        d = min_date.replace(month=((min_date.month-1)//6)*6+1, day=1)
        while d <= max_date:
            end_month = d.month + 5
            end_year = d.year + (end_month-1)//12
            end_month = ((end_month-1) % 12)+1
            end = datetime(end_year, end_month, 1)
            end = (end + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            end = min(end, max_date)
            ranges.append((d, end))
            d = (end + timedelta(days=1)).replace(day=1)
    elif mode == "1Y":
        d = min_date.replace(month=1, day=1)
        while d <= max_date:
            end = datetime(d.year, 12, 31)
            end = min(end, max_date)
            ranges.append((d, end))
            d = datetime(d.year+1, 1, 1)
    return ranges


def copy_schema(src_conn, dest_conn, tables):
    cur = src_conn.cursor()
    for table in tables:
        cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
        row = cur.fetchone()
        if row and row[0]:
            dest_conn.execute(row[0])
    dest_conn.commit()


def partition_db(src_db, outdir, mode):
    os.makedirs(outdir, exist_ok=True)
    with sqlite3.connect(src_db) as conn:
        # Find which tables exist
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        all_tables = set(r[0] for r in cur.fetchall())
        tables = [t for t in TABLES if t in all_tables]
        if not tables:
            print("No known tables found in DB.")
            return
        # Use the first table with data to get date ranges
        for t in tables:
            date_col = TABLES[t][0]
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            if cur.fetchone()[0] > 0:
                date_ranges = get_date_ranges(conn, mode, t, date_col)
                break
        else:
            print("No data in any table.")
            return
        print(
            f"Partitioning {src_db} into {len(date_ranges)} chunks by {mode}...")
        for start, end in date_ranges:
            part_name = f"{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}.db"
            out_db = os.path.join(outdir, part_name)
            with sqlite3.connect(out_db) as out_conn:
                copy_schema(conn, out_conn, tables)
                for table in tables:
                    date_col = TABLES[table][0]
                    q = f"SELECT * FROM {table} WHERE {date_col} >= ? AND {date_col} <= ?"
                    rows = conn.execute(
                        q, (start.strftime(DATE_FORMAT), end.strftime(DATE_FORMAT))).fetchall()
                    if not rows:
                        continue
                    # Get column names
                    col_names = [d[0] for d in conn.execute(
                        f"PRAGMA table_info({table})").fetchall()]
                    placeholders = ','.join(['?']*len(col_names))
                    out_conn.executemany(
                        f"INSERT INTO {table} VALUES ({placeholders})", rows)
                out_conn.commit()
            print(f"Wrote {out_db}")


def main():
    args = parse_args()
    partition_db(args.db, args.outdir, args.mode)


if __name__ == "__main__":
    main()
