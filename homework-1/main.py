"""Скрипт для заполнения данными таблиц в БД Postgres."""
from csv import DictReader
from pathlib import Path
from config import load_config
import psycopg2

conn = psycopg2.connect(**load_config())
try:
    with conn:
        with conn.cursor() as cur:
            with open(Path(__file__).parent / "north_data" / "employees_data.csv") as file:
                data = DictReader(file)
                for row in data:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (row["employee_id"], row["first_name"], row["last_name"],
                                 row["title"], row["birth_date"], row["notes"]))

            with open(Path(__file__).parent / "north_data" / "customers_data.csv") as file:
                data = DictReader(file)
                for row in data:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (row["customer_id"], row["company_name"], row["contact_name"]))

            with open(Path(__file__).parent / "north_data" / "orders_data.csv") as file:
                data = DictReader(file)
                for row in data:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (row["order_id"], row["customer_id"], row["employee_id"],
                                 row["order_date"], row["ship_city"]))
finally:
    conn.close()
