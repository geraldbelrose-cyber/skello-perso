import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, timedelta, datetime

st.set_page_config(page_title="Mini-Skello", layout="wide")
DB_PATH = "data.db"

# ---------------- DB ----------------

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Employés
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)

    # Planning
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shift_date TEXT NOT NULL,
        employee_id INTEGER NOT NULL,
        start_time TEXT,
        end_time TEXT,
        break_minutes INTEGER DEFAULT 0,
        comment TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    """)

    # Congés / Absences
    cur.execute("""
    CREATE TABLE IF NOT EXISTS absences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        employee_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        justified INTEGER NOT NULL DEFAULT 0,
        comment TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    """)

    # Retards
    cur.execute("""
    CREATE TABLE IF NOT EXISTS lateness (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        late_date TEXT NOT NULL,
        employee_id INTEGER NOT NULL,
        scheduled_time TEXT NOT NULL,
        arrival_time TEXT NOT NULL,
        justified INTEGER NOT NULL DEFAULT 0,
        comment TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    """)

    # Heures supplémentaires
    cur.execute("""
    CREATE TABLE IF NOT EXISTS overtime (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_date TEXT NOT NULL,
        employee_id INTEGER NOT NULL,
        minutes INTEGER NOT NULL DEFAULT 0,
        reason TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    """)

    conn.commit()

    # Seed si vide
    cur.execute("SELECT COUNT(*) FROM employees")
    if cur.fetchone()[0] == 0:
        cur.executemany("""
        INSERT INTO employees(first_name,last_name,active)
        VALUES (?,?,1)
        """, [
            ("Employé", "A"),
            ("Employé", "B"),
            ("Employé", "C"),
        ])
        conn.commit()

    conn.close()

init_db()

# ---------------- HELPERS ----------------

ABSENCE_TYPES = ["Congé", "Maladie", "Sans solde", "Autre"]

def monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())

def employees_active():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT id,
               TRIM(first_name || ' ' || last_name) AS full_name
        FROM employees
        WHERE active=1
        ORDER BY id
    """, conn)
    conn.close()
    # label unique: "id - Prénom Nom"
    df["label"] = df["id"].astype(str) + " - " + df["full_name"]
    return df

def employee_options():
    df = employees_active()
    return df["label"].tolist()

def label_to_id(label: str) -> int:
    # "12 - Paul Martin" -> 12
    try:
        return int(str(label).split(" - ", 1)[0].strip())
    except:
        return None

def id_to_label(emp_id: int, options: list[str]) -> str:
    # cherche "id - ..."
    prefix = f"{int(emp_id)} - "
    for o in options:
        if str(o).startswith(prefix):
            return o
    return None

def safe_bool_to_int(v) -> int:
    if v in [True, 1, "1", "true", "True", "TRUE"]:
        return 1
    return 0

def calc_late_minutes(scheduled: str, arrival: str):
    try:
        s = datetime.strptime(scheduled[:5], "%H:%M")
        a = datetime.strptime(arrival[:5], "%H:%M")
        return max(0, int((a - s).total_seconds() // 60))
    except:
        return None

# ---------------- DATA ACCESS ----------------

def shifts_df(d1: date, d2: date):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT s.id,
               s.shift_date,
               s.employee_id,
               TRIM(e.first_name || ' ' || e.last_name) AS employee_name,
               s.start_time,
               s.end_time,
               s.break_minutes,
               s.comment
        FROM shifts s
        JOIN employees e ON e.id = s.employee_id
        WHERE date(s.shift_date) BETWEEN date(?) AND date(?)
        ORDER BY date(s.shift_date), s.employee_id
    """, conn, params=(d1.isoformat(), d2.isoformat()))
    conn.close()
    return df

def save_shifts(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()

    # On remplace la table entière pour rester simple/robuste
    cur.execute("DELETE FROM shifts")

    for _, r in df.iterrows():
        if pd.isna(r.get("shift_date")) or pd.isna(r.get("employee_label")):
            continue

        emp_id = label_to_id(r["employee_label"])
        if emp_id is None:
            continue

        stt = r.get("start_time")
        ett = r.get("end_time")
        brk = r.get("break_minutes")

        cur.execute("""
            INSERT INTO shifts
            (shift_date, employee_id, start_time, end_time, break_minutes, comment)
            VALUES (?,?,?,?,?,?)
        """, (
            str(r["shift_date"])[:10],
            int(emp_id),
            None if pd.isna(stt) else str(stt)[:5],
            None if pd.isna(ett) else str(ett)[:5],
            int(0 if pd.isna(brk) else brk),
            None if pd.isna(r.get("comment")) else str(r.get("comment")),
        ))

    conn.commit()
    conn.close()

def absences_df(d1: date, d2: date):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT a.id,
               a.start_date,
               a.end_date,
               a.employee_id,
               TRIM(e.first_name || ' ' || e.last_name) AS employee_name,
               a.type,
               a.justified,
               a.comment
        FROM absences a
        JOIN employees e ON e.id = a.employee_id
        WHERE date(a.end_date) >= date(?) AND date(a.start_date) <= date(?)
        ORDER BY date(a.start_date), a.employee_id
    """, conn, params=(d1.isoformat(), d2.isoformat()))
    conn.close()
    return df

def save_absences(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM absences")

    for _, r in df.iterrows():
        if pd.isna(r.get("start_date")) or pd.isna(r.get("end_date")) or pd.isna(r.get("employee_label")):
            continue

        emp_id = label_to_id(r["employee_label"])
        if emp_id is None:
            continue

        typ = r.get("type")
        if pd.isna(typ) or str(typ).strip() == "":
            typ = "Congé"

        cur.execute("""
            INSERT INTO absences
            (start_date, end_date, employee_id, type, justified, comment)
            VALUES (?,?,?,?,?,?)
        """, (
            str(r["start_date"])[:10],
            str(r["end_date"])[:10],
            int(emp_id),
            str(typ),
            safe_bool_to_int(r.get("justified")),
            None if pd.isna(r.get("comment")) else str(r.get("comment")),
        ))

    conn.commit()
    conn.close()

def lateness_df(d1: date, d2: date):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT l.id,
               l.late_date,
               l.employee_id,
               TRIM(e.first_name || ' ' || e.last_name) AS employee_name,
               l.scheduled_time,
               l.arrival_time,
               l.justified,
               l.comment
        FROM lateness l
        JOIN employees e ON e.id = l.employee_id
        WHERE date(l.late_date) BETWEEN date(?) AND date(?)
        ORDER BY date(l.late_date), l.employee_id
    """, conn, params=(d1.isoformat(), d2.isoformat()))
    conn.close()
    return df

def save_lateness(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM lateness")

    for _, r in df.iterrows():
        if pd.isna(r.get("late_date")) or pd.isna(r.get("employee_label")):
            continue

        emp_id = label_to_id(r["employee_label"])
        if emp_id is None:
            continue

        sched = str(r.get("scheduled_time") or "")[:5]
        arr = str(r.get("arrival_time") or "")[:5]
        if len(sched) < 4 or len(arr) < 4:
            continue

        cur.execute("""
            INSERT INTO lateness
            (late_date, employee_id, scheduled_time, arrival_time, justified, comment)
            VALUES (?,?,?,?,?,?)
        """, (
            str(r["late_date"])[:10],
            int(emp_id),
            sched,
            arr,
            safe_bool_to_int(r.get("justified")),
            None if pd.isna(r.get("comment")) else str(r.get("comment")),
        ))

    conn.commit()
    conn.close()

def overtime_df(d1: date, d2: date):
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT o.id,
               o.ot_date,
               o.employee_id,
               TRIM(e.first_name || ' ' || e.last_name) AS employee_name,
               o.minutes,
               o.reason
        FROM overtime o
        JOIN employees e ON e.id = o.employee_id
        WHERE date(o.ot_date) BETWEEN date(?) AND date(?)
        ORDER BY date(o.ot_date), o.employee_id
    """, conn, params=(d1.isoformat(), d2.isoformat()))
    conn.close()
    return df

def save_overtime(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM overtime")

    for _, r in df.iterrows():
        if pd.isna(r.get("ot_date")) or pd.isna(r.get("employee_label")):
            continue

        emp_id = label_to_id(r["employee_label"])
        if emp_id is None:
            continue

        mins = r.get("minutes")
        try:
            mins_val = int(mins)
        except:
            mins_val = 0

        cur.execute("""
            INSERT INTO overtime
            (ot_date, employee_id, minutes, reason)
            VALUES (?,?,?,?)
        """, (
            str(r["ot_date"])[:10],
            int(emp_id),
            int(max(0, mins_val)),
            None if pd.isna(r.get("reason")) else str(r.get("reason")),
        ))

    conn.commit()
    conn.close()

# ---------------- UI ----------------

st.title("Mini-Skello — Planning, congés, retards, heures sup")

tab_planning, tab_conges, tab_retards, tab_heures_sup, tab_emps = st.tabs(
    ["Planning", "Congés", "Retards", "Heures sup", "Employés"]
)

options = employee_options()

# ---------------- PLANNING (Semaine) ----------------
with tab_planning:
    st.subheader("Planning (par semaine)")

    week_start = st.date_input(
        "Semaine (lundi)",
        value=monday_of(date.today()),
        key="week_start"
    )
    if week_start.weekday() != 0:
        st.warning("Choisis un lundi (la semaine = lun → dim).")

    d1 = week_start
    d2 = week_start + timedelta(days=6)

    df = shifts_df(d1, d2)

    # Transform: employee_id -> employee_label
    if not df.empty:
        df["employee_label"] = df["employee_id"].apply(lambda x: id_to_label(x, options))
    else:
        df["employee_label"] = pd.Series(dtype="object")

    # On masque employee_id/employee_name dans l’éditeur (on garde seulement employee_label)
    show = df[["id","shift_date","employee_label","start_time","end_time","break_minutes","comment"]].copy()

    edited = st.data_editor(
        show,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("id", disabled=True),
            "shift_date": st.column_config.DateColumn("Date"),
            "employee_label": st.column_config.SelectboxColumn("Employé", options=options),
            "start_time": st.column_config.TextColumn("Début (HH:MM)"),
            "end_time": st.column_config.TextColumn("Fin (HH:MM)"),
            "break_minutes": st.column_config.NumberColumn("Pause (min)"),
            "comment": st.column_config.TextColumn("Commentaire"),
        }
    )

    if st.button("Enregistrer le planning"):
        save_shifts(edited)
        st.success("✅ Planning enregistré")

# ---------------- CONGÉS ----------------
with tab_conges:
    st.subheader("Congés / Absences")

    c1, c2 = st.columns(2)
    with c1:
        a_from = st.date_input("Du", value=date.today() - timedelta(days=30), key="a_from")
    with c2:
        a_to = st.date_input("Au", value=date.today() + timedelta(days=30), key="a_to")

    df = absences_df(a_from, a_to)

    if not df.empty:
        df["employee_label"] = df["employee_id"].apply(lambda x: id_to_label(x, options))
    else:
        df["employee_label"] = pd.Series(dtype="object")

    show = df[["id","start_date","end_date","employee_label","type","justified","comment"]].copy()

    edited = st.data_editor(
        show,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("id", disabled=True),
            "start_date": st.column_config.DateColumn("Début"),
            "end_date": st.column_config.DateColumn("Fin"),
            "employee_label": st.column_config.SelectboxColumn("Employé", options=options),
            "type": st.column_config.SelectboxColumn("Type", options=ABSENCE_TYPES),
            "justified": st.column_config.CheckboxColumn("Justifié"),
            "comment": st.column_config.TextColumn("Commentaire"),
        }
    )

    if st.button("Enregistrer les congés"):
        save_absences(edited)
        st.success("✅ Congés enregistrés")

# ---------------- RETARDS ----------------
with tab_retards:
    st.subheader("Retards (par jour)")

    c1, c2 = st.columns(2)
    with c1:
        l_from = st.date_input("Du", value=date.today() - timedelta(days=30), key="l_from")
    with c2:
        l_to = st.date_input("Au", value=date.today() + timedelta(days=30), key="l_to")

    df = lateness_df(l_from, l_to)

    if not df.empty:
        df["employee_label"] = df["employee_id"].apply(lambda x: id_to_label(x, options))
        df["retard_min"] = df.apply(
            lambda r: calc_late_minutes(str(r.get("scheduled_time","")), str(r.get("arrival_time",""))),
            axis=1
        )
    else:
        df["employee_label"] = pd.Series(dtype="object")
        df["retard_min"] = pd.Series(dtype="int")

    show = df[["id","late_date","employee_label","scheduled_time","arrival_time","retard_min","justified","comment"]].copy()

    edited = st.data_editor(
        show,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("id", disabled=True),
            "late_date": st.column_config.DateColumn("Date"),
            "employee_label": st.column_config.SelectboxColumn("Employé", options=options),
            "scheduled_time": st.column_config.TextColumn("Heure prévue (HH:MM)"),
            "arrival_time": st.column_config.TextColumn("Arrivée (HH:MM)"),
            "retard_min": st.column_config.NumberColumn("Retard (min)", disabled=True),
            "justified": st.column_config.CheckboxColumn("Justifié"),
            "comment": st.column_config.TextColumn("Commentaire"),
        }
    )

    if st.button("Enregistrer les retards"):
        save_lateness(edited)
        st.success("✅ Retards enregistrés")

# ---------------- HEURES SUP ----------------
with tab_heures_sup:
    st.subheader("Heures supplémentaires (par jour)")

    c1, c2 = st.columns(2)
    with c1:
        o_from = st.date_input("Du", value=date.today() - timedelta(days=30), key="o_from")
    with c2:
        o_to = st.date_input("Au", value=date.today() + timedelta(days=30), key="o_to")

    df = overtime_df(o_from, o_to)

    if not df.empty:
        df["employee_label"] = df["employee_id"].apply(lambda x: id_to_label(x, options))
    else:
        df["employee_label"] = pd.Series(dtype="object")

    show = df[["id","ot_date","employee_label","minutes","reason"]].copy()

    edited = st.data_editor(
        show,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("id", disabled=True),
            "ot_date": st.column_config.DateColumn("Date"),
            "employee_label": st.column_config.SelectboxColumn("Employé", options=options),
            "minutes": st.column_config.NumberColumn("Minutes sup", min_value=0, step=15),
            "reason": st.column_config.TextColumn("Raison / Note"),
        }
    )

    if st.button("Enregistrer les heures sup"):
        save_overtime(edited)
        st.success("✅ Heures sup enregistrées")

# ---------------- EMPLOYÉS ----------------
with tab_emps:
    st.subheader("Employés (prénom + nom)")

    conn = get_conn()
    df = pd.read_sql_query("SELECT id, first_name, last_name, active FROM employees ORDER BY id", conn)
    conn.close()

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("ID"),
            "first_name": st.column_config.TextColumn("Prénom"),
            "last_name": st.column_config.TextColumn("Nom"),
            "active": st.column_config.CheckboxColumn("Actif"),
        }
    )

    if st.button("Enregistrer les employés"):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM employees")

        for _, r in edited.iterrows():
            fn = r.get("first_name")
            ln = r.get("last_name")
            if pd.isna(fn) or pd.isna(ln) or str(fn).strip() == "" or str(ln).strip() == "":
                continue

            # id peut être vide -> autoincrement
            rid = r.get("id")
            rid = None if pd.isna(rid) else int(rid)

            cur.execute("""
                INSERT INTO employees(id, first_name, last_name, active)
                VALUES (?,?,?,?)
            """, (
                rid,
                str(fn).strip(),
                str(ln).strip(),
                safe_bool_to_int(r.get("active")),
            ))

        conn.commit()
        conn.close()
        st.success("✅ Employés enregistrés. Rafraîchis la page (ou redémarre l’app) pour mettre à jour les menus.")
