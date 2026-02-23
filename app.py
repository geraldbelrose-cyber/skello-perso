
import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, date, time, timedelta

st.set_page_config(page_title="Mini-Skello (3 employés)", layout="wide")

DB_PATH = "data.db"

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS shifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        shift_date TEXT NOT NULL,
        employee_id INTEGER NOT NULL,
        start_time TEXT NOT NULL,
        end_time TEXT NOT NULL,
        break_minutes INTEGER NOT NULL DEFAULT 0,
        replacement INTEGER NOT NULL DEFAULT 0,
        replaces_employee_id INTEGER,
        comment TEXT,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    """)
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
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM employees;")
    if cur.fetchone()[0] == 0:
        cur.executemany("INSERT INTO employees(id,name,active) VALUES (?,?,1);",
                        [(1,"Employé A"), (2,"Employé B"), (3,"Employé C")])
        conn.commit()

    defaults = {
        "weekday_start": "07:30",
        "weekday_end": "16:30",
        "weekday_break": "60",
        "sat_start": "07:30",
        "sat_end": "12:30",
        "sat_break": "0",
        "rest_emp_1": "WEDNESDAY",
        "rest_emp_2": "THURSDAY",
        "rest_emp_3": "TUESDAY",
        "sat_off_emp_1": "3",
        "sat_off_emp_2": "2",
        "sat_off_emp_3": "4",
    }
    cur.execute("SELECT COUNT(*) FROM settings;")
    if cur.fetchone()[0] == 0:
        cur.executemany("INSERT INTO settings(key,value) VALUES (?,?);", list(defaults.items()))
        conn.commit()
    conn.close()

def get_settings():
    conn = get_conn()
    df = pd.read_sql_query("SELECT key, value FROM settings;", conn)
    conn.close()
    return dict(zip(df["key"], df["value"]))

def set_setting(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO settings(key,value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value;",
        (key, str(value))
    )
    conn.commit()
    conn.close()

def employees_df(active_only=True):
    conn = get_conn()
    q = "SELECT id, name, active FROM employees"
    if active_only:
        q += " WHERE active=1"
    q += " ORDER BY id;"
    df = pd.read_sql_query(q, conn)
    conn.close()
    return df

DAYS = ["MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY","SATURDAY","SUNDAY"]
DAY_LABELS_FR = {
    "MONDAY":"Lundi","TUESDAY":"Mardi","WEDNESDAY":"Mercredi","THURSDAY":"Jeudi",
    "FRIDAY":"Vendredi","SATURDAY":"Samedi","SUNDAY":"Dimanche"
}

def parse_hhmm(s: str) -> time:
    return datetime.strptime(s, "%H:%M").time()

def combine(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute)

def minutes_between(d: date, start: str, end: str) -> int:
    s = combine(d, parse_hhmm(start))
    e = combine(d, parse_hhmm(end))
    return int((e - s).total_seconds() // 60)

def nth_saturday_of_month(d: date) -> int:
    if d.weekday() != 5:
        raise ValueError("Not a Saturday")
    first = date(d.year, d.month, 1)
    offset = (5 - first.weekday()) % 7
    first_sat = first + timedelta(days=offset)
    return 1 + (d - first_sat).days // 7

def daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def shifts_df(date_from: date, date_to: date):
    conn = get_conn()
    q = """
    SELECT s.id, s.shift_date, s.employee_id, e.name as employee,
           s.start_time, s.end_time, s.break_minutes,
           s.replacement, s.replaces_employee_id, s.comment
    FROM shifts s
    JOIN employees e ON e.id = s.employee_id
    WHERE date(s.shift_date) BETWEEN date(?) AND date(?)
    ORDER BY date(s.shift_date), s.employee_id;
    """
    df = pd.read_sql_query(q, conn, params=(date_from.isoformat(), date_to.isoformat()))
    conn.close()
    return df

def save_shifts(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    ids = [int(x) for x in df["id"].dropna().tolist()] if "id" in df.columns else []
    if ids:
        cur.execute(f"DELETE FROM shifts WHERE id IN ({','.join(['?']*len(ids))});", ids)
    for _, r in df.iterrows():
        if pd.isna(r.get("shift_date")) or pd.isna(r.get("employee_id")):
            continue
        cur.execute("""
        INSERT INTO shifts(shift_date,employee_id,start_time,end_time,break_minutes,replacement,replaces_employee_id,comment)
        VALUES (?,?,?,?,?,?,?,?);
        """, (
            str(r["shift_date"])[:10],
            int(r["employee_id"]),
            str(r["start_time"])[:5],
            str(r["end_time"])[:5],
            int(r["break_minutes"]),
            int(r["replacement"]),
            None if pd.isna(r.get("replaces_employee_id")) or r.get("replaces_employee_id")=="" else int(r["replaces_employee_id"]),
            None if pd.isna(r.get("comment")) else str(r.get("comment")),
        ))
    conn.commit()
    conn.close()

def absences_df(date_from: date, date_to: date):
    conn = get_conn()
    q = """
    SELECT a.id, a.start_date, a.end_date, a.employee_id, e.name as employee,
           a.type, a.justified, a.comment
    FROM absences a
    JOIN employees e ON e.id = a.employee_id
    WHERE date(a.end_date) >= date(?) AND date(a.start_date) <= date(?)
    ORDER BY date(a.start_date), a.employee_id;
    """
    df = pd.read_sql_query(q, conn, params=(date_from.isoformat(), date_to.isoformat()))
    conn.close()
    return df

def save_absences(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    ids = [int(x) for x in df["id"].dropna().tolist()] if "id" in df.columns else []
    if ids:
        cur.execute(f"DELETE FROM absences WHERE id IN ({','.join(['?']*len(ids))});", ids)
    for _, r in df.iterrows():
        if pd.isna(r.get("start_date")) or pd.isna(r.get("end_date")) or pd.isna(r.get("employee_id")):
            continue
        cur.execute("""
        INSERT INTO absences(start_date,end_date,employee_id,type,justified,comment)
        VALUES (?,?,?,?,?,?);
        """, (
            str(r["start_date"])[:10],
            str(r["end_date"])[:10],
            int(r["employee_id"]),
            str(r["type"]),
            int(r["justified"]),
            None if pd.isna(r.get("comment")) else str(r.get("comment")),
        ))
    conn.commit()
    conn.close()

def lateness_df(date_from: date, date_to: date):
    conn = get_conn()
    q = """
    SELECT l.id, l.late_date, l.employee_id, e.name as employee,
           l.scheduled_time, l.arrival_time, l.justified, l.comment
    FROM lateness l
    JOIN employees e ON e.id = l.employee_id
    WHERE date(l.late_date) BETWEEN date(?) AND date(?)
    ORDER BY date(l.late_date), l.employee_id;
    """
    df = pd.read_sql_query(q, conn, params=(date_from.isoformat(), date_to.isoformat()))
    conn.close()
    return df

def save_lateness(df: pd.DataFrame):
    conn = get_conn()
    cur = conn.cursor()
    ids = [int(x) for x in df["id"].dropna().tolist()] if "id" in df.columns else []
    if ids:
        cur.execute(f"DELETE FROM lateness WHERE id IN ({','.join(['?']*len(ids))});", ids)
    for _, r in df.iterrows():
        if pd.isna(r.get("late_date")) or pd.isna(r.get("employee_id")):
            continue
        cur.execute("""
        INSERT INTO lateness(late_date,employee_id,scheduled_time,arrival_time,justified,comment)
        VALUES (?,?,?,?,?,?);
        """, (
            str(r["late_date"])[:10],
            int(r["employee_id"]),
            str(r["scheduled_time"])[:5],
            str(r["arrival_time"])[:5],
            int(r["justified"]),
            None if pd.isna(r.get("comment")) else str(r.get("comment")),
        ))
    conn.commit()
    conn.close()

def generate_week(week_start: date):
    s = get_settings()
    emps = employees_df(active_only=True)

    weekday_start = s["weekday_start"]
    weekday_end = s["weekday_end"]
    weekday_break = int(s["weekday_break"])
    sat_start = s["sat_start"]
    sat_end = s["sat_end"]
    sat_break = int(s["sat_break"])

    rows = []
    for i in range(6):  # Mon..Sat
        d = week_start + timedelta(days=i)
        dow = DAYS[d.weekday()]
        for _, e in emps.iterrows():
            emp_id = int(e["id"])
            rest = s.get(f"rest_emp_{emp_id}", "SUNDAY")
            sat_off = int(s.get(f"sat_off_emp_{emp_id}", "3"))

            if dow == rest:
                continue

            if dow == "SATURDAY":
                if nth_saturday_of_month(d) == sat_off:
                    continue
                rows.append((d.isoformat(), emp_id, sat_start, sat_end, sat_break))
            else:
                rows.append((d.isoformat(), emp_id, weekday_start, weekday_end, weekday_break))

    conn = get_conn()
    cur = conn.cursor()
    for shift_date, emp_id, stt, ett, brk in rows:
        cur.execute("SELECT COUNT(*) FROM shifts WHERE shift_date=? AND employee_id=?;", (shift_date, emp_id))
        if cur.fetchone()[0] == 0:
            cur.execute(
                "INSERT INTO shifts(shift_date,employee_id,start_time,end_time,break_minutes,replacement,replaces_employee_id,comment) "
                "VALUES (?,?,?,?,?,0,NULL,'');",
                (shift_date, emp_id, stt, ett, brk)
            )
    conn.commit()
    conn.close()

init_db()
st.title("Mini‑Skello (3 employés) — Planning, absences, retards, heures")

tabs = st.tabs(["Planning", "Absences", "Retards", "Rapports", "Paramètres"])

with tabs[0]:
    st.subheader("Planning")
    colA, colB, colC = st.columns([1,1,2])
    with colA:
        week_start = st.date_input("Semaine (lundi)", value=date.today() - timedelta(days=date.today().weekday()))
        if week_start.weekday() != 0:
            st.warning("Choisis un lundi pour générer correctement la semaine.")
        if st.button("Générer la semaine (Mon→Sam)"):
            if week_start.weekday() == 0:
                generate_week(week_start)
                st.success("Semaine générée (tu peux modifier les horaires après).")
            else:
                st.error("La date doit être un lundi.")
    with colB:
        date_from = st.date_input("Du", value=week_start)
        date_to = st.date_input("Au", value=week_start + timedelta(days=6))
    with colC:
        st.caption("Tu peux modifier les cellules puis cliquer sur **Enregistrer**.")

    df = shifts_df(date_from, date_to)
# --- SAFE TYPES pour éviter les bugs DateColumn/TimeColumn ---
df_display = df.copy()

for c in ["shift_date", "start_time", "end_time"]:
    if c in df_display.columns:
        df_display[c] = df_display[c].astype(str)

for c in ["break_minutes", "employee_id", "replaces_employee_id"]:
    if c in df_display.columns:
        df_display[c] = pd.to_numeric(df_display[c], errors="coerce")

edited = st.data_editor(
    df_display,
    use_container_width=True,
    num_rows="dynamic",
)
        },
        
    )
    if st.button("Enregistrer (Planning)"):
        save_cols = ["id","shift_date","employee_id","start_time","end_time","break_minutes","replacement","replaces_employee_id","comment"]
        save_shifts(edited[save_cols].copy())
        st.success("Planning enregistré.")

with tabs[1]:
    st.subheader("Absences & congés")
    col1, col2 = st.columns(2)
    with col1:
        a_from = st.date_input("Afficher à partir de", value=date.today() - timedelta(days=30), key="a_from")
    with col2:
        a_to = st.date_input("jusqu'à", value=date.today() + timedelta(days=30), key="a_to")

    df = absences_df(a_from, a_to)
    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "id": st.column_config.NumberColumn("id", disabled=True),
            "start_date": st.column_config.DateColumn("Début"),
            "end_date": st.column_config.DateColumn("Fin"),
            "employee_id": st.column_config.NumberColumn("Employé (ID)"),
            "employee": st.column_config.TextColumn("Nom", disabled=True),
            "type": st.column_config.SelectboxColumn("Type", options=["Congé", "Maladie", "Sans solde", "Autre"]),
            "justified": st.column_config.CheckboxColumn("Justifié"),
            "comment": st.column_config.TextColumn("Commentaire"),
        },
        disabled=["employee"]
    )
    if st.button("Enregistrer (Absences)"):
        save_cols = ["id","start_date","end_date","employee_id","type","justified","comment"]
        save_absences(edited[save_cols].copy())
        st.success("Absences enregistrées.")

with tabs[2]:
    st.subheader("Retards")
    col1, col2 = st.columns(2)
    with col1:
        l_from = st.date_input("Afficher à partir de", value=date.today() - timedelta(days=30), key="l_from")
    with col2:
        l_to = st.date_input("jusqu'à", value=date.today() + timedelta(days=30), key="l_to")

    df = lateness_df(l_from, l_to)

    def compute_late(row):
        try:
            sched = datetime.strptime(row["scheduled_time"], "%H:%M")
            arr = datetime.strptime(row["arrival_time"], "%H:%M")
            return max(0, int((arr - sched).total_seconds()//60))
        except:
            return None

    if not df.empty:
        df["retard_min"] = df.apply(compute_late, axis=1)
    else:
        df["retard_min"] = pd.Series(dtype="int")

    edited = st.data_editor(
        df,
        use_container_width=True,
        num_rows="dynamic",
    )
    if st.button("Enregistrer (Retards)"):
        save_cols = ["id","late_date","employee_id","scheduled_time","arrival_time","justified","comment"]
        save_lateness(edited[save_cols].copy())
        st.success("Retards enregistrés.")

with tabs[3]:
    st.subheader("Rapports")
    st.caption("Somme des heures prévues (planning) + absences + retards sur une période.")
    col1, col2 = st.columns(2)
    with col1:
        r_from = st.date_input("Du", value=date.today().replace(day=1), key="r_from")
    with col2:
        r_to = st.date_input("Au", value=date.today(), key="r_to")

    sh = shifts_df(r_from, r_to)
    ab = absences_df(r_from, r_to)
    la = lateness_df(r_from, r_to)

    emps = employees_df(active_only=True)

    def shift_minutes(row):
        d = datetime.strptime(row["shift_date"], "%Y-%m-%d").date()
        mins = minutes_between(d, row["start_time"], row["end_time"])
        return max(0, mins - int(row["break_minutes"]))

    if not sh.empty:
        tmp = sh.copy()
        tmp["planned_min"] = tmp.apply(shift_minutes, axis=1)
        planned = tmp.groupby("employee_id", as_index=False)["planned_min"].sum()
    else:
        planned = pd.DataFrame({"employee_id": [], "planned_min": []})

    abs_min = {int(eid): 0 for eid in emps["id"].tolist()}
    if not ab.empty:
        for _, r in ab.iterrows():
            eid = int(r["employee_id"])
            sd = datetime.strptime(r["start_date"], "%Y-%m-%d").date()
            ed = datetime.strptime(r["end_date"], "%Y-%m-%d").date()
            for d in daterange(sd, ed):
                day_shift = sh[(sh["employee_id"]==eid) & (sh["shift_date"]==d.isoformat())]
                if not day_shift.empty:
                    for __, sr in day_shift.iterrows():
                        abs_min[eid] += shift_minutes(sr)

    late_min = {int(eid): 0 for eid in emps["id"].tolist()}
    if not la.empty:
        for _, r in la.iterrows():
            eid = int(r["employee_id"])
            try:
                sched = datetime.strptime(r["scheduled_time"], "%H:%M")
                arr = datetime.strptime(r["arrival_time"], "%H:%M")
                late_min[eid] += max(0, int((arr - sched).total_seconds()//60))
            except:
                pass

    rep = emps[["id","name"]].rename(columns={"id":"employee_id"})
    rep = rep.merge(planned, on="employee_id", how="left")
    rep["planned_min"] = rep["planned_min"].fillna(0)
    rep["absence_min"] = rep["employee_id"].map(abs_min).fillna(0)
    rep["late_min"] = rep["employee_id"].map(late_min).fillna(0)
    rep["heures_prévues"] = (rep["planned_min"]/60).round(2)
    rep["absences_h"] = (rep["absence_min"]/60).round(2)
    rep["retards_h"] = (rep["late_min"]/60).round(2)
    rep["heures_restantes"] = (rep["planned_min"]/60 - rep["absence_min"]/60 - rep["late_min"]/60).round(2)

    st.dataframe(rep[["employee_id","name","heures_prévues","absences_h","retards_h","heures_restantes"]], use_container_width=True)
    csv = rep.to_csv(index=False).encode("utf-8")
    st.download_button("Télécharger le rapport (CSV)", data=csv, file_name="rapport_heures.csv", mime="text/csv")

with tabs[4]:
    st.subheader("Paramètres (horaires & repos modifiables)")
    s = get_settings()

    st.markdown("### Horaires par défaut")
    c1, c2, c3 = st.columns(3)
    with c1:
        weekday_start = st.text_input("Début semaine (Lun–Ven) HH:MM", value=s["weekday_start"])
        sat_start = st.text_input("Début samedi HH:MM", value=s["sat_start"])
    with c2:
        weekday_end = st.text_input("Fin semaine (Lun–Ven) HH:MM", value=s["weekday_end"])
        sat_end = st.text_input("Fin samedi HH:MM", value=s["sat_end"])
    with c3:
        weekday_break = st.number_input("Pause semaine (minutes)", min_value=0, max_value=240, value=int(s["weekday_break"]))
        sat_break = st.number_input("Pause samedi (minutes)", min_value=0, max_value=240, value=int(s["sat_break"]))

    st.markdown("### Jours de repos (1 jour / semaine) + 1 samedi off / mois")
    emps = employees_df(active_only=True)
    for _, e in emps.iterrows():
        eid = int(e["id"])
        st.markdown(f"**{e['name']} (ID {eid})**")
        col1, col2 = st.columns(2)
        rest_key = f"rest_emp_{eid}"
        rest_default = s.get(rest_key, "SUNDAY")
        with col1:
            rest_val = st.selectbox(
                "Jour de repos hebdo",
                options=DAYS,
                index=DAYS.index(rest_default),
                key=f"rest_{eid}",
                format_func=lambda x: DAY_LABELS_FR.get(x, x),
            )
        sat_key = f"sat_off_emp_{eid}"
        sat_default = int(s.get(sat_key, "3"))
        with col2:
            sat_val = st.selectbox("Samedi off du mois", options=[1,2,3,4,5], index=[1,2,3,4,5].index(sat_default), key=f"sat_{eid}")
        st.divider()

    if st.button("Enregistrer les paramètres"):
        set_setting("weekday_start", weekday_start)
        set_setting("weekday_end", weekday_end)
        set_setting("weekday_break", weekday_break)
        set_setting("sat_start", sat_start)
        set_setting("sat_end", sat_end)
        set_setting("sat_break", sat_break)
        for _, e in emps.iterrows():
            eid = int(e["id"])
            set_setting(f"rest_emp_{eid}", st.session_state.get(f"rest_{eid}"))
            set_setting(f"sat_off_emp_{eid}", st.session_state.get(f"sat_{eid}"))
        st.success("Paramètres enregistrés. Retourne sur Planning pour (re)générer une semaine.")
