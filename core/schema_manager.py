# core/schema_manager.py
import sqlite3
from typing import List, Dict

from constants.schema import SCHEMAS


def create_table_from_schema(conn: sqlite3.Connection, schema_name: str):
    if schema_name not in SCHEMAS:
        raise ValueError(f"Schema '{schema_name}' not found in SCHEMAS")

    schema = SCHEMAS[schema_name]
    table_name = schema["table_name"]
    columns = schema["columns"]
    indexes = schema.get("indexes", [])

    # ✅ 각 요소별 strip() 적용하여 공백 제거
    col_defs = []
    for col, typ, constraint in columns:
        col = col.strip()
        typ = typ.strip()
        constraint = constraint.strip() if constraint else ""
        col_def = f"{col} {typ}"
        if constraint:
            col_def += f" {constraint}"
        col_defs.append(col_def)

    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
    conn.execute(create_sql)

    for idx_def in indexes:
        if len(idx_def) == 2:
            idx_name, col = idx_def
            idx_name = idx_name.strip()
            col = col.strip()
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({col})"
        else:
            idx_name, col, condition = idx_def
            idx_name = idx_name.strip()
            col = col.strip()
            condition = condition.strip() if condition else ""
            sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({col})"
            if condition:
                sql += f" {condition}"
        conn.execute(sql)


def get_insert_sql(table_name: str, columns: List[str]) -> str:
    placeholders = ', '.join(['?'] * len(columns))
    col_list = ', '.join(columns)
    return f"INSERT OR REPLACE INTO {table_name} ({col_list}) VALUES ({placeholders})"


def build_row_from_dict(record: Dict, columns: List[str]) -> tuple:
    return tuple(record.get(col) for col in columns)


def save_batch(conn: sqlite3.Connection, table_name: str,
               columns: List[str], batch: List[Dict]) -> None:
    if not batch:
        return

    sql = get_insert_sql(table_name, columns)
    rows = [build_row_from_dict(r, columns) for r in batch]
    conn.executemany(sql, rows)
    