# -*- coding: utf-8 -*-
"""
Producer GUI (multi-topic, multi-mode) with DB monitoring:
- Start streaming: topics.status -> 'active'
- Stop streaming : topics.status -> 'approved'
- Admin deactivation auto-stops producer
"""

import os, json, time, random, threading
import pandas as pd
import yfinance as yf
import mysql.connector
from flask import Flask, render_template_string, request, jsonify
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ================== CONFIG ==================
DB_CONFIG = {
    "host": "172.29.25.56",  # MySQL host (admin machine)
    "user": "team",
    "password": "team137",
    "database": "kafka_stream"
}
KAFKA_BROKER = "172.29.70.143:9092"  # Kafka broker
UPLOAD_DIR = "./uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ================== APP/STATE ==================
app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_DIR

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

ACTIVE = {}  # topic -> {thread, stop_event, mode, ...}
LOG_BUF = []


def log(msg: str):
    print(msg)
    LOG_BUF.append(msg)
    if len(LOG_BUF) > 500:
        del LOG_BUF[:250]


# ================== DB HELPERS ==================
def db():  
    return mysql.connector.connect(buffered=True, autocommit=True, **DB_CONFIG)


def get_approved_topics():
    con = db(); cur = con.cursor()
    cur.execute("SELECT name FROM topics WHERE status='approved'")
    rows = [r[0] for r in cur.fetchall()]
    cur.close(); con.close()
    return rows


def set_topic_status(topic: str, status: str):
    """Update topic status in DB."""
    try:
        con = db(); cur = con.cursor()
        cur.execute("UPDATE topics SET status=%s WHERE name=%s", (status, topic))
        con.commit(); cur.close(); con.close()
        return True
    except Exception as e:
        log(f"⚠️ DB update error (set {status}): {e}")
        return False


def request_topic_pending(name: str):
    """Request topic (pending)."""
    try:
        con = db(); cur = con.cursor()
        cur.execute("INSERT IGNORE INTO topics(name, status) VALUES(%s,'pending')", (name,))
        con.commit(); cur.close(); con.close()
        return True
    except Exception as e:
        log(f"⚠️ Could not request topic: {e}")
        return False


def store_log(topic: str, msg: dict):
    """Store producer message logs (keep last 20 per topic)."""
    try:
        con = db(); cur = con.cursor()
        cur.execute(
            "INSERT INTO producer_logs (topic_name, message) VALUES (%s,%s)",
            (topic, json.dumps(msg))
        )
        con.commit()
        cur.execute("""
            DELETE FROM producer_logs
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id FROM producer_logs
                    WHERE topic_name=%s
                    ORDER BY id DESC
                    LIMIT 20
                ) x
            ) AND topic_name=%s
        """, (topic, topic))
        con.commit(); cur.close(); con.close()
    except Exception as e:
        log(f"⚠️ DB log error: {e}")


# ================== KAFKA HELPERS ==================
def ensure_topic(topic: str):
    """Create Kafka topic if not already exists."""
    admin = None
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
        log(f"✅ Created Kafka topic {topic}")
    except TopicAlreadyExistsError:
        log(f"ℹ️ Topic '{topic}' already exists.")
    except Exception as e:
        log(f"⚠️ Topic creation failed: {e}")
    finally:
        try:
            if admin: admin.close()
        except:
            pass


# ================== STREAMERS ==================
def stream_random(topic, columns, interval, stop_event):
    cols = columns or ["temperature", "humidity", "pressure", "value"]
    while not stop_event.is_set():
        msg = {}
        for c in cols:
            lc = c.lower()
            if "temp" in lc: msg[c] = round(random.uniform(20, 35), 2)
            elif "hum" in lc: msg[c] = random.randint(30, 90)
            elif "press" in lc: msg[c] = round(random.uniform(950, 1050), 2)
            else: msg[c] = round(random.uniform(0, 100), 2)
        msg["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
        producer.send(topic, msg)
        store_log(topic, msg)
        log(f"📤 Random → {topic}: {msg}")
        time.sleep(interval)


def stream_csv(topic, file_path, columns, interval, stop_event):
    try:
        df = pd.read_csv(file_path)
        if columns:
            df = df[[c for c in df.columns if c in columns]]
    except Exception as e:
        log(f"⚠️ CSV read error for '{topic}': {e}")
        return
    for _, row in df.iterrows():
        if stop_event.is_set(): break
        msg = {k: (None if pd.isna(v) else str(v)) for k, v in row.items()}
        msg["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
        producer.send(topic, msg)
        store_log(topic, msg)
        log(f"📤 CSV → {topic}: {msg}")
        time.sleep(interval)
    log(f"✅ Completed CSV streaming for {topic}")


def stream_yfinance(topic, ticker, columns, interval, stop_event):
    base = ["open", "high", "low", "close", "volume"]
    cols = [c for c in (columns or base) if c in base]
    if not cols: cols = base
    while not stop_event.is_set():
        try:
            data = yf.download(ticker, period="1d", interval="1m").tail(1)
            if not data.empty:
                row = data.iloc[0]
                full = {
                    "symbol": ticker,
                    "open": float(row.get("Open", 0.0)),
                    "high": float(row.get("High", 0.0)),
                    "low": float(row.get("Low", 0.0)),
                    "close": float(row.get("Close", 0.0)),
                    "volume": int(row.get("Volume", 0)),
                }
                msg = {"symbol": ticker}
                for c in cols: msg[c] = full[c]
                msg["timestamp"] = time.strftime('%Y-%m-%d %H:%M:%S')
                producer.send(topic, msg)
                store_log(topic, msg)
                log(f"📤 yF → {topic}: {msg}")
        except Exception as e:
            log(f"⚠️ yFinance error ({topic}): {e}")
        time.sleep(interval)


# ================== STREAM CONTROL ==================
def start_topic_stream(cfg):
    topic = cfg["topic"].strip()
    mode = cfg["mode"]
    interval = float(cfg.get("interval", 3))
    columns = cfg.get("columns") or []
    file_path = cfg.get("file_path")
    ticker = cfg.get("ticker")

    approved = set(get_approved_topics())
    if topic not in approved:
        log(f"⛔ Topic '{topic}' not approved. Request Admin approval first.")
        return {"ok": False, "msg": "Topic not approved by Admin."}

    set_topic_status(topic, "active")
    ensure_topic(topic)

    if topic in ACTIVE and not ACTIVE[topic]["stop_event"].is_set():
        return {"ok": False, "msg": f"Topic '{topic}' already streaming."}

    stop_event = threading.Event()
    if mode == "random":
        t = threading.Thread(target=stream_random, args=(topic, columns, interval, stop_event), daemon=True)
    elif mode == "csv":
        if not file_path or not os.path.exists(file_path):
            return {"ok": False, "msg": "CSV file missing."}
        t = threading.Thread(target=stream_csv, args=(topic, file_path, columns, interval, stop_event), daemon=True)
    elif mode == "yfinance":
        if not ticker:
            return {"ok": False, "msg": "Ticker required for yFinance."}
        t = threading.Thread(target=stream_yfinance, args=(topic, ticker, columns, interval, stop_event), daemon=True)
    else:
        return {"ok": False, "msg": "Invalid mode."}

    ACTIVE[topic] = {
        "thread": t, "stop_event": stop_event,
        "mode": mode, "columns": columns,
        "interval": interval, "file_path": file_path, "ticker": ticker
    }
    t.start()
    log(f"▶️ Started {mode} stream → {topic} (every {interval}s)")
    return {"ok": True, "msg": f"Started {topic}"}


def stop_topic_stream(topic: str):
    topic = topic.strip()
    if topic not in ACTIVE:
        set_topic_status(topic, "approved")
        return {"ok": False, "msg": f"Topic '{topic}' not active. Marked APPROVED."}

    ACTIVE[topic]["stop_event"].set()
    log(f"⏹️ Stop requested → {topic}")
    set_topic_status(topic, "approved")
    del ACTIVE[topic]
    return {"ok": True, "msg": f"Stopped {topic}"}


# ================== DB MONITOR THREAD ==================
def monitor_db_for_deactivation():
    """Auto-stop producer if Admin deactivates (sets status='approved')."""
    while True:
        try:
            if ACTIVE:
                con = db(); cur = con.cursor()
                cur.execute("SELECT name, status FROM topics WHERE status IN ('approved','active')")
                rows = dict(cur.fetchall())  # {topic: status}
                cur.close(); con.close()

                for topic in list(ACTIVE.keys()):
                    if rows.get(topic) == "approved":
                        log(f"⚠️ Admin deactivated topic '{topic}' — stopping producer.")
                        stop_topic_stream(topic)
        except Exception as e:
            log(f"⚠️ DB monitor error: {e}")

        time.sleep(3)  # poll every 3s


# ================== HTML UI ==================
PAGE = """
<!doctype html>
<html>
<head>
  <title>Multi-Topic Producer</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .mono{font-family:ui-monospace, Menlo, Consolas, monospace}
    .card{border-radius:16px}
    .colbox{max-height:160px;overflow:auto;border:1px solid #e5e7eb;padding:8px;border-radius:8px;}
  </style>
</head>
<body class="p-4">
<div class="container">
  <h3 class="mb-3">📦 Kafka Producer — Multi-Topic</h3>

  <!-- Topic Config Creator -->
  <div class="card mb-3">
    <div class="card-body">
      <h6 class="mb-3">Create a Topic Configuration</h6>
      <div class="row g-3 align-items-end">
        <div class="col-md-2">
          <label class="form-label">Mode</label>
          <select id="mode" class="form-select">
            <option value="random">Random</option>
            <option value="csv">CSV</option>
            <option value="yfinance">yFinance</option>
          </select>
        </div>
        <div class="col-md-3">
          <label class="form-label">Topic</label>
          <input id="topic" class="form-control" placeholder="topic_name">
          <div class="form-text"><a href="#" onclick="requestTopic()">Request Topic (pending)</a></div>
        </div>
        <div class="col-md-2">
          <label class="form-label">Interval (sec)</label>
          <input id="interval" class="form-control" value="3">
        </div>
        <div class="col-md-5 text-end">
          <button class="btn btn-outline-secondary" onclick="refreshApproved()">Refresh Approved</button>
          <button class="btn btn-primary" onclick="addConfig()">Add Config</button>
        </div>
      </div>

      <div id="mode-panels" class="mt-3">
        <div id="panel-random">
          <h6>Random Columns</h6>
          <div id="randomCols" class="colbox mb-2"></div>
          <div class="input-group">
            <input id="customCol" class="form-control" placeholder="Add custom column">
            <button class="btn btn-outline-primary" onclick="addCustomCol()">+</button>
          </div>
        </div>

        <div id="panel-csv" style="display:none;">
          <h6>CSV Upload → Preview Columns</h6>
          <div class="row g-2">
            <div class="col-md-8"><input type="file" id="csvfile" class="form-control"></div>
            <div class="col-md-4"><button class="btn btn-primary w-100" onclick="uploadCSV()">Upload</button></div>
          </div>
          <div id="csvCols" class="colbox mt-2"></div>
        </div>

        <div id="panel-yf" style="display:none;">
          <h6>yFinance</h6>
          <div class="row g-2 mb-2">
            <div class="col-md-6"><input id="ticker" class="form-control" value="TCS.NS"></div>
            <div class="col-md-6"><button class="btn btn-primary w-100" onclick="loadYfCols()">Show Columns</button></div>
          </div>
          <div id="yfCols" class="colbox"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Approved topics -->
  <div class="card mb-3">
    <div class="card-body">
      <h6>🟢 Approved Topics</h6>
      <ul id="approvedList" class="mb-0"></ul>
    </div>
  </div>

  <!-- Config List -->
  <div class="card mb-3">
    <div class="card-body">
      <h6 class="mb-2">Topic Configurations</h6>
      <div id="cfgList"></div>
    </div>
  </div>

  <!-- Logs -->
  <div class="card">
    <div class="card-body">
      <h6>📜 Live Logs</h6>
      <pre id="logs" class="mono" style="max-height:420px;overflow:auto;"></pre>
    </div>
  </div>
</div>

<script>
let RANDOM_DEFAULTS = ["temperature","humidity","pressure","value"];
let random_selected = new Set(RANDOM_DEFAULTS);
let csv_selected = new Set();
let yf_selected = new Set();
let csv_file_path = null;

let CONFIGS = []; // array of {mode, topic, interval, columns, file_path?, ticker?}

function renderRandomCols(){
  const box = document.getElementById('randomCols');
  box.innerHTML = RANDOM_DEFAULTS.map(c=>`
    <div class="form-check form-check-inline">
      <input class="form-check-input" type="checkbox" value="${c}" checked onchange="toggleRandomCol(this)">
      <label class="form-check-label">${c}</label>
    </div>
  `).join('');
}
function toggleRandomCol(el){ if(el.checked){random_selected.add(el.value)} else {random_selected.delete(el.value)} }
function addCustomCol(){
  const v = document.getElementById('customCol').value.trim();
  if(!v) return;
  RANDOM_DEFAULTS.push(v);
  random_selected.add(v);
  document.getElementById('customCol').value = "";
  renderRandomCols();
}

function modeChange(){
  const m = document.getElementById('mode').value;
  document.getElementById('panel-random').style.display = (m==="random"?"block":"none");
  document.getElementById('panel-csv').style.display = (m==="csv"?"block":"none");
  document.getElementById('panel-yf').style.display = (m==="yfinance"?"block":"none");
}
document.getElementById('mode').addEventListener('change', modeChange);

function uploadCSV(){
  const f = document.getElementById('csvfile').files[0];
  if(!f){ alert("Pick a CSV first"); return; }
  const fd = new FormData(); fd.append("csvfile", f);
  fetch("/api/upload_csv",{method:"POST", body:fd})
    .then(r=>r.json()).then(d=>{
      if(!d.ok){ alert("Upload failed: "+d.error); return; }
      csv_file_path = d.file_path;
      const cols = d.columns || [];
      csv_selected = new Set(cols);
      document.getElementById('csvCols').innerHTML = cols.map(c=>`
        <div class="form-check form-check-inline">
          <input class="form-check-input" type="checkbox" value="${c}" checked onchange="toggleCsvCol(this)">
          <label class="form-check-label">${c}</label>
        </div>
      `).join('');
    });
}
function toggleCsvCol(el){ if(el.checked){csv_selected.add(el.value)} else {csv_selected.delete(el.value)} }

function loadYfCols(){
  fetch("/api/yf_columns").then(r=>r.json()).then(d=>{
    const cols = d.columns || [];
    yf_selected = new Set(cols);
    document.getElementById('yfCols').innerHTML = cols.map(c=>`
      <div class="form-check form-check-inline">
        <input class="form-check-input" type="checkbox" value="${c}" checked onchange="toggleYfCol(this)">
        <label class="form-check-label">${c}</label>
      </div>
    `).join('');
  });
}
function toggleYfCol(el){ if(el.checked){yf_selected.add(el.value)} else {yf_selected.delete(el.value)} }

function addConfig(){
  const mode = document.getElementById('mode').value;
  const topic = document.getElementById('topic').value.trim();
  const interval = parseFloat(document.getElementById('interval').value || "3");
  if(!topic){ alert("Topic is required"); return; }

  let cfg = {mode, topic, interval};
  if(mode==="random"){
    cfg.columns = Array.from(random_selected);
  }else if(mode==="csv"){
    if(!csv_file_path){ alert("Upload a CSV first"); return; }
    cfg.file_path = csv_file_path;
    cfg.columns = Array.from(csv_selected);
  }else{
    const t = document.getElementById('ticker').value.trim() || "TCS.NS";
    cfg.ticker = t;
    cfg.columns = Array.from(yf_selected);
  }

  CONFIGS.push(cfg);
  renderCfgList();
  clearTopicInputs();
}

function clearTopicInputs(){ document.getElementById('topic').value=""; }

function renderCfgList(){
  const box = document.getElementById('cfgList');
  if(CONFIGS.length===0){ box.innerHTML = "<div class='text-muted'>No configurations yet.</div>"; return; }
  box.innerHTML = CONFIGS.map((c,i)=>`
    <div class="border rounded p-2 mb-2">
      <div class="d-flex justify-content-between align-items-center">
        <div>
          <strong>${c.topic}</strong> — <span class="badge bg-secondary">${c.mode}</span> — every ${c.interval}s
          <div class="small text-muted">cols: ${(c.columns && c.columns.length)? c.columns.join(", "): "all"}</div>
          ${c.mode==="csv" ? `<div class="small text-muted">file: ${c.file_path}</div>` : ""}
          ${c.mode==="yfinance" ? `<div class="small text-muted">ticker: ${c.ticker}</div>` : ""}
        </div>
        <div>
          <button class="btn btn-success btn-sm me-2" onclick="startTopic(${i})">Start</button>
          <button class="btn btn-danger btn-sm" onclick="stopTopic('${c.topic}')">Stop</button>
        </div>
      </div>
    </div>
  `).join('');
}

function startTopic(i){
  const cfg = CONFIGS[i];
  fetch("/api/start_topic", {
    method:"POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify(cfg)
  }).then(r=>r.json()).then(d=>{
    alert(d.msg || (d.ok? "Started":"Failed"));
    fetchActive(); refreshApproved();
  });
}
function stopTopic(topic){
  fetch("/api/stop_topic?topic="+encodeURIComponent(topic))
   .then(r=>r.json()).then(d=>{
     alert(d.msg || "Stopped");
     fetchActive(); refreshApproved();
   });
}
function fetchActive(){
  fetch("/api/active").then(r=>r.json()).then(d=>{
    console.log("ACTIVE:", d);
  });
}
function refreshApproved(){
  fetch("/api/approved").then(r=>r.json()).then(d=>{
    const ul = document.getElementById('approvedList');
    ul.innerHTML = (d.topics||[]).map(t=>`<li>${t}</li>`).join('') || "<li>None</li>";
  });
}
function requestTopic(){
  const t = document.getElementById('topic').value.trim();
  if(!t){ alert("Enter a topic first"); return; }
  fetch("/api/request_topic", {
    method:"POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({topic:t})
  }).then(r=>r.json()).then(d=> alert(d.msg));
}
function pullLogs(){
  fetch("/api/logs").then(r=>r.text()).then(t=>{
    const el = document.getElementById('logs');
    const atBottom = (el.scrollTop + el.clientHeight + 10) >= el.scrollHeight;
    el.textContent = t;
    if(atBottom){ el.scrollTop = el.scrollHeight; }
  });
}

renderRandomCols();
modeChange();
refreshApproved();
renderCfgList();
pullLogs();
setInterval(pullLogs, 1000);
</script>
</body>
</html>
"""

# ================== ROUTES ==================
@app.get("/")
def home():
    return render_template_string(PAGE)


@app.get("/api/approved")
def api_approved():
    return {"topics": get_approved_topics()}


@app.post("/api/request_topic")
def api_request_topic():
    data = request.get_json(silent=True) or {}
    topic = (data.get("topic") or "").strip()
    if not topic:
        return {"ok": False, "msg": "Topic required"}, 400
    ok = request_topic_pending(topic)
    return {"ok": ok, "msg": "Requested as pending — ask Admin to approve." if ok else "Failed"}


@app.post("/api/upload_csv")
def api_upload_csv():
    f = request.files.get("csvfile")
    if not f or not f.filename:
        return {"ok": False, "error": "No file uploaded"}, 400
    path = os.path.join(app.config["UPLOAD_FOLDER"], f.filename)
    f.save(path)
    try:
        df = pd.read_csv(path, nrows=1)
        cols = list(df.columns)
        log(f"🧾 CSV uploaded: {path} cols={cols}")
        return {"ok": True, "file_path": path, "columns": cols}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 400


@app.get("/api/yf_columns")
def api_yf_columns():
    return {"ok": True, "columns": ["open", "high", "low", "close", "volume"]}


@app.post("/api/start_topic")
def api_start_topic():
    cfg = request.get_json(silent=True) or {}
    res = start_topic_stream(cfg)
    return (res, 200) if res.get("ok") else (res, 400)


@app.get("/api/stop_topic")
def api_stop_topic():
    topic = request.args.get("topic", "").strip()
    res = stop_topic_stream(topic)
    return (res, 200) if res.get("ok") else (res, 400)


@app.get("/api/active")
def api_active():
    status = {
        t: {
            "mode": v["mode"],
            "interval": v["interval"],
            "columns": v["columns"],
            "alive": v["thread"].is_alive()
        } for t, v in ACTIVE.items()
    }
    return {"active": status}


@app.get("/api/logs")
def api_logs():
    return "\n".join(LOG_BUF[-500:])


# ================== RUN ==================
if __name__ == "__main__":
    try:
        con = db(); cur = con.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS producer_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                topic_name VARCHAR(255) NOT NULL,
                message JSON,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.commit(); cur.close(); con.close()
    except Exception as e:
        log(f"⚠️ Could not ensure producer_logs table: {e}")

    threading.Thread(target=monitor_db_for_deactivation, daemon=True).start()
    log("🧠 DB monitor thread started to detect admin deactivations.")
    app.run(host="0.0.0.0", port=5001, debug=True)
