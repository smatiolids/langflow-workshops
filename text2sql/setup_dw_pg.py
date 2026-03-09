#!/usr/bin/env python3
"""Create a PostgreSQL "mini data warehouse" (Sales + Accounts Receivable)
and load random sample data with N records per fact table.

Usage:
  python setup_dw_pg.py --n 5000 --seed 42
  # reads PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE from .env (if present)

Notes:
- Creates a star schema with 5 dims + 2 facts.
- Inserts dim rows first, then inserts:
  - N rows into fact_sales (line-level)
  - N rows into fact_ar (invoice-level, tied to invoice_id, customer, dates)
- Requires a PostgreSQL driver (`psycopg`) and a reachable database.
"""

from __future__ import annotations

import argparse
import os
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import psycopg


DDL = """
-- =========================
-- Dimensions
-- =========================

CREATE TABLE IF NOT EXISTS dim_date (
  date_key        INTEGER PRIMARY KEY,   -- yyyymmdd, e.g. 20260126
  full_date       TEXT NOT NULL,          -- 'YYYY-MM-DD'
  year            INTEGER NOT NULL,
  quarter         INTEGER NOT NULL,
  month           INTEGER NOT NULL,
  day             INTEGER NOT NULL,
  week_of_year    INTEGER,
  day_name        TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key    SERIAL PRIMARY KEY,
  customer_id     TEXT UNIQUE NOT NULL,   -- business key
  customer_name   TEXT NOT NULL,
  segment         TEXT,
  city            TEXT,
  state           TEXT,
  country         TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_key     SERIAL PRIMARY KEY,
  product_id      TEXT UNIQUE NOT NULL,   -- business key
  product_name    TEXT NOT NULL,
  category        TEXT,
  brand           TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
  store_key       SERIAL PRIMARY KEY,
  store_id        TEXT UNIQUE NOT NULL,   -- business key
  store_name      TEXT NOT NULL,
  channel         TEXT,                   -- e.g. 'ecommerce', 'retail', 'b2b'
  city            TEXT,
  state           TEXT
);

CREATE TABLE IF NOT EXISTS dim_payment_terms (
  payment_terms_key SERIAL PRIMARY KEY,
  terms_code        TEXT UNIQUE NOT NULL,  -- e.g. 'NET30'
  description       TEXT,
  net_days          INTEGER NOT NULL
);

-- =========================
-- Facts
-- =========================

CREATE TABLE IF NOT EXISTS fact_sales (
  sales_id        SERIAL PRIMARY KEY,

  invoice_id      TEXT NOT NULL,
  order_id        TEXT,
  line_number     INTEGER NOT NULL,

  invoice_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
  customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),
  product_key     INTEGER NOT NULL REFERENCES dim_product(product_key),
  store_key       INTEGER NOT NULL REFERENCES dim_store(store_key),
  payment_terms_key INTEGER REFERENCES dim_payment_terms(payment_terms_key),

  quantity        REAL NOT NULL,
  unit_price      REAL NOT NULL,
  discount_amount REAL DEFAULT 0,
  tax_amount      REAL DEFAULT 0,

  gross_amount    REAL NOT NULL,          -- quantity * unit_price
  net_amount      REAL NOT NULL           -- gross - discount + tax
);

CREATE TABLE IF NOT EXISTS fact_ar (
  ar_id           SERIAL PRIMARY KEY,

  invoice_id      TEXT NOT NULL,           -- ties to sales
  customer_key    INTEGER NOT NULL REFERENCES dim_customer(customer_key),

  invoice_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
  due_date_key     INTEGER NOT NULL REFERENCES dim_date(date_key),
  payment_date_key INTEGER REFERENCES dim_date(date_key),

  payment_terms_key INTEGER REFERENCES dim_payment_terms(payment_terms_key),

  invoice_amount  REAL NOT NULL,
  amount_paid     REAL NOT NULL DEFAULT 0,
  amount_open     REAL NOT NULL,           -- invoice_amount - amount_paid
  status          TEXT NOT NULL             -- 'OPEN', 'PAID', 'OVERDUE', 'PARTIAL'
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_sales_invoice ON fact_sales(invoice_id);
CREATE INDEX IF NOT EXISTS idx_sales_date ON fact_sales(invoice_date_key);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_ar_status ON fact_ar(status);
CREATE INDEX IF NOT EXISTS idx_ar_due_date ON fact_ar(due_date_key);
"""


@dataclass(frozen=True)
class DimKeys:
    date_keys: List[int]
    customer_keys: List[int]
    product_keys: List[int]
    store_keys: List[int]
    payment_terms_keys: List[int]
    payment_terms_by_key: Dict[int, int]  # payment_terms_key -> net_days


def load_env_file(path: str) -> Dict[str, str]:
    env_path = Path(path).expanduser().resolve()
    if not env_path.exists():
        return {}

    values: Dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if len(value) >= 2 and ((value[0] == value[-1]) and value[0] in {'"', "'"}):
            value = value[1:-1]

        values[key] = value

    return values


def env_or_default(env: Dict[str, str], keys: List[str], default: str = "") -> str:
    for key in keys:
        if key in os.environ and os.environ[key] != "":
            return os.environ[key]
        if key in env and env[key] != "":
            return env[key]
    return default


def parse_args() -> argparse.Namespace:
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("--env-file", default=".env", help="Path to .env file with PostgreSQL connection vars")
    pre_args, remaining = bootstrap.parse_known_args()

    env = load_env_file(pre_args.env_file)

    p = argparse.ArgumentParser()
    p.add_argument("--env-file", default=pre_args.env_file, help="Path to .env file with PostgreSQL connection vars")
    p.add_argument(
        "--dsn",
        default=env_or_default(env, ["PG_DSN", "DATABASE_URL"], pre_args.__dict__.get("dsn", "")),
        help="Optional connection URI (e.g. postgresql://user:pass@host:5432/dbname)",
    )
    p.add_argument("--host", default=env_or_default(env, ["PGHOST", "DB_HOST"], "localhost"), help="PostgreSQL host")
    p.add_argument("--port", type=int, default=int(env_or_default(env, ["PGPORT", "DB_PORT"], "5432")), help="PostgreSQL port")
    p.add_argument("--user", default=env_or_default(env, ["PGUSER", "DB_USER"], "postgres"), help="PostgreSQL user")
    p.add_argument("--password", default=env_or_default(env, ["PGPASSWORD", "DB_PASSWORD"], ""), help="PostgreSQL password")
    p.add_argument("--dbname", default=env_or_default(env, ["PGDATABASE", "DB_NAME"], "sales_dw"), help="PostgreSQL database name")
    p.add_argument("--n", type=int, default=1000, help="N records for EACH fact table (fact_sales and fact_ar)")
    p.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    p.add_argument("--start-date", default="2025-01-01", help="Start date (YYYY-MM-DD) for dim_date range")
    p.add_argument("--days", type=int, default=730, help="How many days to generate in dim_date")
    p.add_argument("--customers", type=int, default=200, help="Number of customers")
    p.add_argument("--products", type=int, default=300, help="Number of products")
    p.add_argument("--stores", type=int, default=30, help="Number of stores")

    args = p.parse_args(remaining)
    return args


def yyyymmdd(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def connect(args: argparse.Namespace) -> psycopg.Connection:
    if args.dsn:
        return psycopg.connect(args.dsn)

    kwargs = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "dbname": args.dbname,
    }
    if args.password:
        kwargs["password"] = args.password
    return psycopg.connect(**kwargs)


def create_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for statement in [s.strip() for s in DDL.split(";") if s.strip()]:
            cur.execute(statement)
    conn.commit()


def seed_dim_date(conn: psycopg.Connection, start: date, days: int) -> List[int]:
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        dk = yyyymmdd(d)
        iso_week = int(d.strftime("%V"))
        day_name = d.strftime("%A")
        rows.append((dk, d.isoformat(), d.year, quarter(d.month), d.month, d.day, iso_week, day_name))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_date(
              date_key, full_date, year, quarter, month, day, week_of_year, day_name
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_key) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    return [r[0] for r in rows]


def seed_dim_customers(conn: psycopg.Connection, n: int, rng: random.Random) -> List[int]:
    segments = ["SMB", "Mid-Market", "Enterprise"]
    cities = ["São Paulo", "Campinas", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre", "Recife"]
    states = ["SP", "RJ", "MG", "PR", "RS", "PE"]
    rows = []
    for i in range(1, n + 1):
        cid = f"CUST{i:05d}"
        rows.append((cid, f"Customer {i:05d}", rng.choice(segments), rng.choice(cities), rng.choice(states), "BR"))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_customer(
              customer_id, customer_name, segment, city, state, country
            ) VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            rows,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT customer_key FROM dim_customer")
        return [r[0] for r in cur.fetchall()]


def seed_dim_products(conn: psycopg.Connection, n: int, rng: random.Random) -> List[int]:
    categories = ["Electronics", "Accessories", "Software", "Services", "Office"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
    rows = []
    for i in range(1, n + 1):
        pid = f"PROD{i:05d}"
        rows.append((pid, f"Product {i:05d}", rng.choice(categories), rng.choice(brands)))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_product(
              product_id, product_name, category, brand
            ) VALUES (%s,%s,%s,%s)
            ON CONFLICT (product_id) DO NOTHING
            """,
            rows,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT product_key FROM dim_product")
        return [r[0] for r in cur.fetchall()]


def seed_dim_stores(conn: psycopg.Connection, n: int, rng: random.Random) -> List[int]:
    channels = ["ecommerce", "retail", "b2b"]
    cities = ["São Paulo", "Campinas", "Rio de Janeiro", "Belo Horizonte", "Curitiba"]
    states = ["SP", "RJ", "MG", "PR"]
    rows = []
    for i in range(1, n + 1):
        sid = f"ST{i:03d}"
        rows.append((sid, f"Store {i:03d}", rng.choice(channels), rng.choice(cities), rng.choice(states)))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_store(
              store_id, store_name, channel, city, state
            ) VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (store_id) DO NOTHING
            """,
            rows,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT store_key FROM dim_store")
        return [r[0] for r in cur.fetchall()]


def seed_dim_payment_terms(conn: psycopg.Connection) -> Tuple[List[int], Dict[int, int]]:
    terms = [
        ("NET7", "Net 7 days", 7),
        ("NET15", "Net 15 days", 15),
        ("NET30", "Net 30 days", 30),
        ("NET45", "Net 45 days", 45),
        ("NET60", "Net 60 days", 60),
    ]

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO dim_payment_terms(terms_code, description, net_days)
            VALUES (%s,%s,%s)
            ON CONFLICT (terms_code) DO NOTHING
            """,
            terms,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT payment_terms_key, net_days FROM dim_payment_terms")
        rows = cur.fetchall()
    keys = [r[0] for r in rows]
    by_key = {k: nd for k, nd in rows}
    return keys, by_key


def build_dim_keys(conn: psycopg.Connection, args: argparse.Namespace, rng: random.Random) -> DimKeys:
    start = date.fromisoformat(args.start_date)
    date_keys = seed_dim_date(conn, start, args.days)
    customer_keys = seed_dim_customers(conn, args.customers, rng)
    product_keys = seed_dim_products(conn, args.products, rng)
    store_keys = seed_dim_stores(conn, args.stores, rng)
    payment_terms_keys, payment_terms_by_key = seed_dim_payment_terms(conn)
    return DimKeys(
        date_keys=date_keys,
        customer_keys=customer_keys,
        product_keys=product_keys,
        store_keys=store_keys,
        payment_terms_keys=payment_terms_keys,
        payment_terms_by_key=payment_terms_by_key,
    )


def clear_facts(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM fact_sales;")
        cur.execute("DELETE FROM fact_ar;")
    conn.commit()


def gen_invoice_id(i: int) -> str:
    return f"INV{i:08d}"


def gen_order_id(i: int) -> str:
    return f"ORD{i:08d}"


def load_fact_sales(conn: psycopg.Connection, dims: DimKeys, n: int, rng: random.Random) -> List[str]:
    """
    Inserts N sales line rows. Returns list of invoice_ids used.
    We'll reuse invoice ids, but each row is a line. Some invoices will have multiple lines.
    """
    invoice_ids: List[str] = []

    unique_invoices = max(1, int(n / rng.uniform(1.7, 2.5)))
    unique_invoices = min(unique_invoices, n)

    invoice_meta = []
    for inv_i in range(1, unique_invoices + 1):
        inv_id = gen_invoice_id(inv_i)
        inv_date_key = rng.choice(dims.date_keys)
        customer_key = rng.choice(dims.customer_keys)
        store_key = rng.choice(dims.store_keys)
        terms_key = rng.choice(dims.payment_terms_keys)
        order_id = gen_order_id(inv_i)
        invoice_meta.append((inv_id, order_id, inv_date_key, customer_key, store_key, terms_key))

    rows = []
    line_counts: Dict[str, int] = {m[0]: 0 for m in invoice_meta}

    for _ in range(n):
        inv_id, order_id, inv_date_key, customer_key, store_key, terms_key = rng.choice(invoice_meta)
        line_counts[inv_id] += 1
        line_number = line_counts[inv_id]

        product_key = rng.choice(dims.product_keys)

        qty = rng.randint(1, 20)
        unit_price = round(rng.uniform(5, 5000), 2)
        gross = round(qty * unit_price, 2)
        discount = round(gross * rng.uniform(0.0, 0.15), 2)
        tax = round((gross - discount) * rng.uniform(0.0, 0.12), 2)
        net = round(gross - discount + tax, 2)

        rows.append(
            (
                inv_id,
                order_id,
                line_number,
                inv_date_key,
                customer_key,
                product_key,
                store_key,
                terms_key,
                float(qty),
                float(unit_price),
                float(discount),
                float(tax),
                float(gross),
                float(net),
            )
        )
        invoice_ids.append(inv_id)

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO fact_sales(
              invoice_id, order_id, line_number,
              invoice_date_key, customer_key, product_key, store_key, payment_terms_key,
              quantity, unit_price, discount_amount, tax_amount, gross_amount, net_amount
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    conn.commit()
    return invoice_ids


def load_fact_ar(conn: psycopg.Connection, dims: DimKeys, n: int, rng: random.Random) -> None:
    """
    Inserts N AR rows at invoice level.
    We'll create unique invoice_ids in this fact (can overlap with sales invoice_ids conceptually,
    but we keep it independent; for join demos, see sync_ar_with_sales).
    """
    statuses = ["OPEN", "PAID", "OVERDUE", "PARTIAL"]

    # Small cache for date lookup by date_key.
    with conn.cursor() as cur:
        cur.execute("SELECT date_key, full_date FROM dim_date")
        date_lookup = {r[0]: date.fromisoformat(r[1]) for r in cur.fetchall()}

    rows = []
    for i in range(1, n + 1):
        inv_id = gen_invoice_id(10_000_000 + i)
        customer_key = rng.choice(dims.customer_keys)
        terms_key = rng.choice(dims.payment_terms_keys)
        net_days = dims.payment_terms_by_key[terms_key]

        invoice_date_key = rng.choice(dims.date_keys)
        inv_date = date_lookup[invoice_date_key]

        due_date = inv_date + timedelta(days=net_days)
        due_date_key = yyyymmdd(due_date)

        if due_date_key not in set(dims.date_keys):
            due_date_key = max(dims.date_keys)

        invoice_amount = round(rng.uniform(50, 50_000), 2)
        status = rng.choices(population=statuses, weights=[0.35, 0.35, 0.10, 0.20], k=1)[0]

        if status == "PAID":
            amount_paid = invoice_amount
            amount_open = 0.0
            paid_delta = rng.randint(0, max(0, net_days))
            pay_date = inv_date + timedelta(days=paid_delta)
            payment_date_key = yyyymmdd(pay_date)
            if payment_date_key not in set(dims.date_keys):
                payment_date_key = None
        elif status == "PARTIAL":
            amount_paid = round(invoice_amount * rng.uniform(0.1, 0.9), 2)
            amount_open = round(invoice_amount - amount_paid, 2)
            payment_date_key = None
        else:
            amount_paid = 0.0
            amount_open = invoice_amount
            payment_date_key = None

        if status == "OVERDUE":
            payment_date_key = None

        rows.append(
            (
                inv_id,
                customer_key,
                invoice_date_key,
                due_date_key,
                payment_date_key,
                terms_key,
                float(invoice_amount),
                float(amount_paid),
                float(amount_open),
                status,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO fact_ar(
              invoice_id, customer_key,
              invoice_date_key, due_date_key, payment_date_key,
              payment_terms_key,
              invoice_amount, amount_paid, amount_open, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    conn.commit()


def sync_ar_with_sales(
    conn: psycopg.Connection,
    dims: DimKeys,
    sales_invoice_ids: List[str],
    n: int,
    rng: random.Random,
) -> None:
    """
    OPTIONAL helper: instead of independent invoice_ids in AR, generate AR rows using invoice_ids
    found in sales, to make joins more interesting.
    This will delete and reload fact_ar with N rows drawn from sales invoices.
    """
    with conn.cursor() as cur:
        cur.execute("DELETE FROM fact_ar;")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT invoice_id, customer_key, invoice_date_key, payment_terms_key, SUM(net_amount) AS inv_total
            FROM fact_sales
            GROUP BY invoice_id, customer_key, invoice_date_key, payment_terms_key
            """
        )
        inv_totals = cur.fetchall()

    if not inv_totals:
        return

    with conn.cursor() as cur:
        cur.execute("SELECT date_key, full_date FROM dim_date")
        date_lookup = {r[0]: date.fromisoformat(r[1]) for r in cur.fetchall()}

    rows = []
    for invoice_id, customer_key, invoice_date_key, terms_key, inv_total in [rng.choice(inv_totals) for _ in range(n)]:
        net_days = dims.payment_terms_by_key.get(terms_key, 30)

        inv_date = date_lookup[invoice_date_key]
        due_date = inv_date + timedelta(days=net_days)
        due_date_key = yyyymmdd(due_date)
        if due_date_key not in set(dims.date_keys):
            due_date_key = max(dims.date_keys)

        invoice_amount = float(round(inv_total, 2))
        status = rng.choices(["OPEN", "PAID", "OVERDUE", "PARTIAL"], weights=[0.35, 0.35, 0.10, 0.20], k=1)[0]

        if status == "PAID":
            amount_paid = invoice_amount
            amount_open = 0.0
            paid_delta = rng.randint(0, max(0, net_days))
            pay_date = inv_date + timedelta(days=paid_delta)
            payment_date_key = yyyymmdd(pay_date)
            if payment_date_key not in set(dims.date_keys):
                payment_date_key = None
        elif status == "PARTIAL":
            amount_paid = float(round(invoice_amount * rng.uniform(0.1, 0.9), 2))
            amount_open = float(round(invoice_amount - amount_paid, 2))
            payment_date_key = None
        else:
            amount_paid = 0.0
            amount_open = invoice_amount
            payment_date_key = None

        rows.append(
            (
                invoice_id,
                customer_key,
                invoice_date_key,
                due_date_key,
                payment_date_key,
                terms_key,
                invoice_amount,
                amount_paid,
                amount_open,
                status,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO fact_ar(
              invoice_id, customer_key,
              invoice_date_key, due_date_key, payment_date_key,
              payment_terms_key,
              invoice_amount, amount_paid, amount_open, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
    conn.commit()


def print_quick_checks(conn: psycopg.Connection) -> None:
    tables = [
        "dim_date",
        "dim_customer",
        "dim_product",
        "dim_store",
        "dim_payment_terms",
        "fact_sales",
        "fact_ar",
    ]
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            cnt = cur.fetchone()[0]
            print(f"{t}: {cnt}")

        print("\nTop customers by net sales:")
        cur.execute(
            """
            SELECT c.customer_name, ROUND(SUM(s.net_amount)::numeric, 2) AS net_sales
            FROM fact_sales s
            JOIN dim_customer c ON c.customer_key = s.customer_key
            GROUP BY c.customer_name
            ORDER BY net_sales DESC
            LIMIT 5
            """
        )
        for row in cur.fetchall():
            print("  ", row)

        print("\nAR open amount by status:")
        cur.execute(
            """
            SELECT status, ROUND(SUM(amount_open)::numeric, 2) AS open_amount
            FROM fact_ar
            GROUP BY status
            ORDER BY open_amount DESC
            """
        )
        for row in cur.fetchall():
            print("  ", row)


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    conn = connect(args)

    try:
        create_schema(conn)
        dims = build_dim_keys(conn, args, rng)

        # Start clean for facts (dims are idempotent by business keys)
        clear_facts(conn)

        sales_invoice_ids = load_fact_sales(conn, dims, args.n, rng)
        load_fact_ar(conn, dims, args.n, rng)

        # To use sale-derived AR for stronger joins, use:
        # sync_ar_with_sales(conn, dims, sales_invoice_ids, args.n, rng)

        print(f"\n✅ PostgreSQL DW loaded in: {args.dbname}@{args.host}:{args.port}")
        print(f"✅ Loaded N={args.n} rows into fact_sales and fact_ar (each).")
        print_quick_checks(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
