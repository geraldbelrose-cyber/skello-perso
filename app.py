import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, timedelta

st.set_page_config(page_title="Mini-Skello", layout="wide")

DB_PATH = "data.db"

# ---------- DATABASE ----------

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shift_date TEXT NOT NULL,
        employee_id INTEGER NOT NULL,
        start_time TEXT,
        end_time TEXT,
        break_minutes INTEGER DEFAULT 0,
        comment TEXT
    );
    """)

    conn.commit()

    # Ajouter employés par défaut si vide
    cur.execute("SELECT COUNT(*) FROM employees")
    if cur.fetchone()[0] == 0:
        cur.executemany("""
        INSERT INTO employees(first_name,last_name)
        VALUES (?,?)
        """, [
            ("Paul", "Martin"),
            ("Julie", "Durand"),
            ("Lucas", "Petit")
        ])
        conn.commit()

    conn.close()

init_db()

# ---------- FUNCTIONS ----------

def employees_df():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT id,
               first_name || ' ' || last_name AS name
        FROM employees
        WHERE active=1
        ORDER BY id
    """, conn)
    conn.close()
    return df

def shifts_df(d1, d2):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT s.id,
               s.shift_date,
               s.employee_id,
               e.first_name || ' ' || e.last_name AS employee,
               s.start_time,
               s.end_time,
               s.break_minutes,
               s.comment
        FROM shifts s
        JOIN employees e ON e.id = s.employee_id
        WHERE date(s.shift_date) BETWEEN date(?) AND date(?)
        ORDER BY s.shift_date
    """, conn, params=(d1.isoformat(), d2.isoformat()))
    conn.close()
    return df

def save_shifts(df):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM shifts")

    for _, r in df.iterrows():
        if pd.isna(r["shift_date"]) or pd.isna(r["employee_id"]):
            continue

        cur.execute("""
            INSERT INTO shifts
            (shift_date,employee_id,start_time,end_time,break_minutes,comment)
            VALUES (?,?,?,?,?,?)
        """, (
            str(r["shift_date"])[:10],
            int(r["employee_id"]),
            r["start_time"],
            r["end_time"],
            int(r["break_minutes"] or 0),
            r["comment"]
        ))

    conn.commit()
    conn.close()

# ---------- UI ----------

st.title("Mini-Skello — Planning")

tab1, tab2 = st.tabs(["Planning", "Employés"])

# ---------- PLANNING ----------

with tab1:

    week_start = st.date_input(
        "Semaine (lundi)",
        value=date.today() - timedelta(days=date.today().weekday())
    )

    d1 = week_start
    d2 = week_start + timedelta(days=6)

    df = shifts_df(d1, d2)

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic"
    )

    if st.button("Enregistrer le planning"):
        save_shifts(edited)
        st.success("Planning enregistré")

# ---------- EMPLOYEES ----------

with tab2:

    st.subheader("Gestion des employés")

    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT id, first_name, last_name, active FROM employees",
        conn
    )
    conn.close()

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic"
    )

    if st.button("Enregistrer les employés"):
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("DELETE FROM employees")

        for _, r in edited.iterrows():
            cur.execute("""
                INSERT INTO employees(id,first_name,last_name,active)
                VALUES (?,?,?,?)
            """, (
                int(r["id"]),
                r["first_name"],
                r["last_name"],
                int(r["active"])
            ))

        conn.commit()
        conn.close()

        st.success("Employés enregistrés")
