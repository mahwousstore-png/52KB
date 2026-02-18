"""
utils/db_manager.py - v18.0
- تتبع تاريخ الأسعار (يحدث السعر إذا تغير)
- حفظ نقاط استئناف للمعالجة الخلفية
- قرارات لكل منتج (موافق/تأجيل/إزالة)
- سجل كامل بالتاريخ والوقت
"""
import sqlite3, json
from datetime import datetime

DB_PATH = "pricing_v18.db"


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _date():
    return datetime.now().strftime("%Y-%m-%d")


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # أحداث عامة
    c.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, page TEXT,
        event_type TEXT, details TEXT,
        product_name TEXT, action_taken TEXT
    )""")

    # قرارات المستخدم (موافق/تأجيل/إزالة)
    c.execute("""CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, product_name TEXT,
        our_price REAL, comp_price REAL,
        diff REAL, competitor TEXT,
        old_status TEXT, new_status TEXT,
        reason TEXT, decided_by TEXT DEFAULT 'user'
    )""")

    # تاريخ الأسعار لكل منتج عند كل منافس
    c.execute("""CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, product_name TEXT,
        competitor TEXT, price REAL,
        our_price REAL, diff REAL,
        match_score REAL, decision TEXT,
        product_id TEXT DEFAULT ''
    )""")

    # نقطة الاستئناف للمعالجة الخلفية
    c.execute("""CREATE TABLE IF NOT EXISTS job_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE,
        started_at TEXT, updated_at TEXT,
        status TEXT DEFAULT 'running',
        total INTEGER DEFAULT 0,
        processed INTEGER DEFAULT 0,
        results_json TEXT DEFAULT '[]',
        our_file TEXT, comp_files TEXT
    )""")

    # تاريخ التحليلات
    c.execute("""CREATE TABLE IF NOT EXISTS analysis_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, our_file TEXT,
        comp_file TEXT, total_products INTEGER,
        matched INTEGER, missing INTEGER, summary TEXT
    )""")

    # AI cache
    c.execute("""CREATE TABLE IF NOT EXISTS ai_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, prompt_hash TEXT UNIQUE,
        response TEXT, source TEXT
    )""")

    conn.commit()
    conn.close()


# ─── أحداث ────────────────────────────────
def log_event(page, event_type, details="", product_name="", action=""):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO events (timestamp,page,event_type,details,product_name,action_taken) VALUES (?,?,?,?,?,?)",
            (_ts(), page, event_type, details, product_name, action)
        )
        conn.commit(); conn.close()
    except: pass


# ─── قرارات ────────────────────────────────
def log_decision(product_name, old_status, new_status, reason="",
                 our_price=0, comp_price=0, diff=0, competitor=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO decisions
               (timestamp,product_name,our_price,comp_price,diff,competitor,
                old_status,new_status,reason)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (_ts(), product_name, our_price, comp_price, diff,
             competitor, old_status, new_status, reason)
        )
        conn.commit(); conn.close()
    except: pass


def get_decisions(product_name=None, status=None, limit=100):
    try:
        conn = get_db()
        if product_name:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE product_name LIKE ? ORDER BY id DESC LIMIT ?",
                (f"%{product_name}%", limit)
            ).fetchall()
        elif status:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE new_status=? ORDER BY id DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


# ─── تاريخ الأسعار (الميزة الذكية) ──────────
def upsert_price_history(product_name, competitor, price,
                          our_price=0, diff=0, match_score=0,
                          decision="", product_id=""):
    """
    يحفظ السعر اليوم. إذا وُجد سعر سابق لنفس المنتج/المنافس اليوم → يحدّثه.
    إذا كان أمس → يضيف سجلاً جديداً لتتبع التغيير.
    يرجع True إذا تغير السعر عن آخر تسجيل.
    """
    conn = get_db()
    today = _date()

    # آخر سعر مسجل لهذا المنتج/المنافس
    last = conn.execute(
        """SELECT price, date FROM price_history
           WHERE product_name=? AND competitor=?
           ORDER BY id DESC LIMIT 1""",
        (product_name, competitor)
    ).fetchone()

    price_changed = False
    if last:
        last_price = last["price"]
        last_date  = last["date"]
        price_changed = abs(float(price) - float(last_price)) > 0.01

        if last_date == today:
            # نفس اليوم → حدّث فقط
            conn.execute(
                """UPDATE price_history SET price=?,our_price=?,diff=?,
                   match_score=?,decision=?,product_id=?
                   WHERE product_name=? AND competitor=? AND date=?""",
                (price, our_price, diff, match_score, decision,
                 product_id, product_name, competitor, today)
            )
        else:
            # يوم جديد → أضف سجل
            conn.execute(
                """INSERT INTO price_history
                   (date,product_name,competitor,price,our_price,diff,
                    match_score,decision,product_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (today, product_name, competitor, price, our_price,
                 diff, match_score, decision, product_id)
            )
    else:
        # أول مرة
        conn.execute(
            """INSERT INTO price_history
               (date,product_name,competitor,price,our_price,diff,
                match_score,decision,product_id)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (today, product_name, competitor, price, our_price,
             diff, match_score, decision, product_id)
        )

    conn.commit(); conn.close()
    return price_changed


def get_price_history(product_name, competitor="", limit=30):
    try:
        conn = get_db()
        if competitor:
            rows = conn.execute(
                """SELECT * FROM price_history
                   WHERE product_name=? AND competitor=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, competitor, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM price_history WHERE product_name=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, limit)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


def get_price_changes(days=7):
    """منتجات تغير سعرها خلال X يوم"""
    try:
        conn = get_db()
        rows = conn.execute(
            """SELECT p1.product_name, p1.competitor,
                      p1.price as new_price, p2.price as old_price,
                      p1.date as new_date, p2.date as old_date,
                      (p1.price - p2.price) as price_diff
               FROM price_history p1
               JOIN price_history p2
                 ON p1.product_name=p2.product_name
                AND p1.competitor=p2.competitor
                AND p1.id > p2.id
               WHERE p1.date >= date('now', ?)
                 AND abs(p1.price - p2.price) > 0.01
               ORDER BY abs(p1.price - p2.price) DESC
               LIMIT 100""",
            (f"-{days} days",)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


# ─── المعالجة الخلفية ──────────────────────
def save_job_progress(job_id, total, processed, results, status="running",
                      our_file="", comp_files=""):
    conn = get_db()
    conn.execute(
        """INSERT OR REPLACE INTO job_progress
           (job_id,started_at,updated_at,status,total,processed,
            results_json,our_file,comp_files)
           VALUES (?,
               COALESCE((SELECT started_at FROM job_progress WHERE job_id=?), ?),
               ?, ?, ?, ?, ?, ?, ?)""",
        (job_id, job_id, _ts(), _ts(), status, total, processed,
         json.dumps(results, ensure_ascii=False, default=str),
         our_file, comp_files)
    )
    conn.commit(); conn.close()


def get_job_progress(job_id):
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress WHERE job_id=?", (job_id,)
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except: d["results"] = []
            return d
    except: pass
    return None


def get_last_job():
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except: d["results"] = []
            return d
    except: pass
    return None


# ─── سجل التحليلات ─────────────────────────
def log_analysis(our_file, comp_file, total, matched, missing, summary=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO analysis_history
               (timestamp,our_file,comp_file,total_products,matched,missing,summary)
               VALUES (?,?,?,?,?,?,?)""",
            (_ts(), our_file, comp_file, total, matched, missing, summary)
        )
        conn.commit(); conn.close()
    except: pass


def get_analysis_history(limit=20):
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM analysis_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


def get_events(page=None, limit=50):
    try:
        conn = get_db()
        if page:
            rows = conn.execute(
                "SELECT * FROM events WHERE page=? ORDER BY id DESC LIMIT ?",
                (page, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except: return []


init_db()
