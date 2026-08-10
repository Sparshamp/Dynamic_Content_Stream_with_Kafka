import json, time, threading
import mysql.connector
from flask import Flask, render_template_string, request, jsonify
from kafka import KafkaConsumer

# ===================== CONFIG =====================
DB_CONFIG = {
    "host": "172.29.25.56",
    "user": "team",
    "password": "team137",
    "database": "kafka_stream",
}
KAFKA_BROKER = "172.29.70.143:9092"

# ===================== APP & STATE =====================
app = Flask(__name__)

RUN_STATE = {
    "username": None,
    "running": False,
    "thread": None,
    "stop_event": threading.Event(),
    "current_topics": set()
}

# ===================== DB HELPERS =====================
def db():
    return mysql.connector.connect(buffered=True, autocommit=True, **DB_CONFIG)

def get_approved_topics():
    """Return list of (id, name) for approved or active topics."""
    con = db(); cur = con.cursor()
    cur.execute("SELECT id, name FROM topics WHERE status IN ('approved','active')")
    rows = cur.fetchall()
    cur.close(); con.close()
    return rows

def get_user_subscriptions(username):
    """Return set of topic_id the user is subscribed to."""
    con = db(); cur = con.cursor()
    # `user` column in user_subscriptions (backtick to avoid function name conflict)
    cur.execute("""
        SELECT topic_id FROM user_subscriptions
        WHERE `user`=%s
    """, (username,))
    rows = {r[0] for r in cur.fetchall()}
    cur.close(); con.close()
    return rows

def get_user_topics_names(username):
    """Return list of topic names (strings) the user is subscribed to (and approved)."""
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT t.name
        FROM user_subscriptions us
        JOIN topics t ON t.id = us.topic_id
        WHERE us.`user`=%s AND t.status IN ('approved','active')
    """, (username,))
    names = [r[0] for r in cur.fetchall()]
    cur.close(); con.close()
    return names

def subscribe_topic(username, topic_id):
    con = db(); cur = con.cursor()
    # Avoid duplicates
    cur.execute("""
        INSERT IGNORE INTO user_subscriptions(`user`, topic_id)
        VALUES(%s, %s)
    """, (username, topic_id))
    con.commit()
    cur.close(); con.close()

def unsubscribe_topic(username, topic_id):
    con = db(); cur = con.cursor()
    cur.execute("""
        DELETE FROM user_subscriptions
        WHERE `user`=%s AND topic_id=%s
    """, (username, topic_id))
    con.commit()
    cur.close(); con.close()

def store_consumer_log(username, topic_name, message_dict):
    """Insert message and keep only last 20 rows per (username, topic_name)."""
    # message_dict -> JSON
    payload = json.dumps(message_dict)
    con = db(); cur = con.cursor()
    cur.execute("""
        INSERT INTO consumer_logs (username, topic_name, message)
        VALUES (%s, %s, %s)
    """, (username, topic_name, payload))
    con.commit()

    # Keep ONLY last 20 per (username, topic_name)
    cur.execute("""
        DELETE FROM consumer_logs
        WHERE id NOT IN (
            SELECT id FROM (
                SELECT id FROM consumer_logs
                WHERE username=%s AND topic_name=%s
                ORDER BY id DESC
                LIMIT 20
            ) x
        ) AND username=%s AND topic_name=%s
    """, (username, topic_name, username, topic_name))
    con.commit()
    cur.close(); con.close()

def get_recent_logs(username, limit_total=100):
    """Fetch recent logs for the user (up to limit_total across topics)."""
    con = db(); cur = con.cursor()
    cur.execute("""
        SELECT id, topic_name, message, timestamp
        FROM consumer_logs
        WHERE username=%s
        ORDER BY id DESC
        LIMIT %s
    """, (username, limit_total))
    rows = cur.fetchall()
    cur.close(); con.close()

    out = []
    for r in rows:
        try:
            msg = json.loads(r[2]) if isinstance(r[2], str) else r[2]
        except Exception:
            msg = r[2]
        out.append({
            "id": r[0],
            "topic": r[1],
            "message": msg,
            "timestamp": r[3].strftime('%Y-%m-%d %H:%M:%S') if r[3] else None
        })
    # Show newest first
    return out

# ===================== CONSUMER THREAD =====================
def consumer_loop():
    """Background loop: consume only user's subscribed topics, and rebuild when subscriptions change."""
    username = RUN_STATE["username"]
    if not username:
        return

    consumer = None
    try:
        while not RUN_STATE["stop_event"].is_set():
            # get the latest subscribed topic names (approved only)
            wanted_topics = set(get_user_topics_names(username))

            # if changed, (re)create the consumer with new subscriptions
            if wanted_topics != RUN_STATE["current_topics"]:
                RUN_STATE["current_topics"] = wanted_topics
                # Close old consumer if any
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                    consumer = None

                if not wanted_topics:
                    # No topics yet, just wait and retry
                    time.sleep(3)
                    continue

                consumer = KafkaConsumer(
                    *sorted(list(wanted_topics)),
                    bootstrap_servers=KAFKA_BROKER,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    group_id=f"consumer_{username}"
                )
                print(f"🟢 {username} subscribed to: {sorted(list(wanted_topics))}")

            if not consumer:
                time.sleep(2)
                continue

            # Poll for a short interval to allow checking for subscription updates
            msg_pack = consumer.poll(timeout_ms=1000, max_records=50)
            # msg_pack is a dict: {TopicPartition: [ConsumerRecord, ...]}
            for _, records in msg_pack.items():
                for rec in records:
                    try:
                        store_consumer_log(username, rec.topic, rec.value)
                        print(f"💬 [{username}] {rec.topic}: {rec.value}")
                    except Exception as e:
                        print(f"⚠️ Log store error: {e}")

            # small sleep to be friendly
            time.sleep(0.1)

    except Exception as e:
        print(f"❌ Consumer loop exception: {e}")
    finally:
        if consumer:
            try:
                consumer.close()
            except:
                pass
        RUN_STATE["running"] = False
        print("⏹️ Consumer stopped.")

# ===================== HTML =====================
PAGE = """
<!doctype html>
<html>
<head>
  <title>Kafka Consumer Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .mono{font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace}
    .card{border-radius:18px}
    .pill{padding:.2rem .6rem;border-radius:999px;font-size:.8rem}
  </style>
</head>
<body class="p-4">
<div class="container">
  <h3 class="mb-3">🧭 Kafka Consumer Dashboard</h3>

  <!-- Username -->
  <div class="card mb-3">
    <div class="card-body">
      <div class="row g-3 align-items-end">
        <div class="col-md-4">
          <label class="form-label">Username</label>
          <input id="username" class="form-control" placeholder="e.g., shreya">
          <div class="form-text">Used for subscriptions and log grouping</div>
        </div>
        <div class="col-md-8 text-end">
          <button class="btn btn-primary" onclick="setUser()">Set User</button>
          <button class="btn btn-success" onclick="startConsumer()">Start Consuming</button>
          <button class="btn btn-danger" onclick="stopConsumer()">Stop</button>
        </div>
      </div>
      <div class="mt-2">
        <span class="pill bg-light border" id="status">Status: idle</span>
      </div>
    </div>
  </div>

  <!-- Topics -->
  <div class="card mb-3">
    <div class="card-body">
      <div class="d-flex justify-content-between align-items-center">
        <h6 class="mb-0">🟢 Approved Topics</h6>
        <button class="btn btn-outline-secondary btn-sm" onclick="loadTopics()">Refresh</button>
      </div>
      <div id="topics" class="mt-3"></div>
    </div>
  </div>

  <!-- Logs -->
  <div class="card">
    <div class="card-body">
      <div class="d-flex justify-content-between align-items-center">
        <h6 class="mb-0">📜 Latest Messages (last 20 per topic)</h6>
        <button class="btn btn-outline-secondary btn-sm" onclick="loadLogs()">Refresh</button>
      </div>
      <pre id="logs" class="mono mt-2" style="max-height:420px;overflow:auto;"></pre>
    </div>
  </div>
</div>

<script>
let CURRENT_USER = null;

function setUser(){
  const u = document.getElementById('username').value.trim();
  if(!u){ alert("Enter username"); return; }
  fetch("/api/set_user", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({username:u})})
    .then(r=>r.json()).then(d=>{
      if(d.ok){
        CURRENT_USER = u;
        document.getElementById('status').textContent = "Status: user=" + u + (d.running?" (running)":" (stopped)");
        loadTopics();
        loadLogs();
      } else {
        alert(d.msg||"Failed");
      }
    });
}

function loadTopics(){
  if(!CURRENT_USER){ return; }
  fetch("/api/topics?username="+encodeURIComponent(CURRENT_USER))
    .then(r=>r.json()).then(d=>{
      const box = document.getElementById('topics');
      if(!d.ok){ box.innerHTML = "<div class='text-muted'>No topics</div>"; return; }
      box.innerHTML = d.topics.map(t=>{
        const btn = t.subscribed
          ? `<button class="btn btn-sm btn-outline-danger" onclick="toggleSub(${t.id}, false)">Unsubscribe</button>`
          : `<button class="btn btn-sm btn-outline-primary" onclick="toggleSub(${t.id}, true)">Subscribe</button>`;
        return `
          <div class="d-flex align-items-center justify-content-between border rounded p-2 mb-2">
            <div><strong>${t.name}</strong> <span class="text-muted">(#${t.id})</span></div>
            <div>${btn}</div>
          </div>
        `;
      }).join('') || "<div class='text-muted'>No approved topics</div>";
    });
    // if a topic was previously subscribed but now is not:
	if(t.subscribed === false && t.id in PREVIOUS_SUBS) {
    	alert("Admin unsubscribed you from " + t.name);
	}
}

function toggleSub(topic_id, sub){
  if(!CURRENT_USER){ alert("Set username first"); return; }
  const url = sub ? "/api/subscribe" : "/api/unsubscribe";
  fetch(url, {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({username: CURRENT_USER, topic_id: topic_id})
  }).then(r=>r.json()).then(d=>{
    if(!d.ok){ alert(d.msg || "Failed"); return; }
    loadTopics();
  });
}

function startConsumer(){
  if(!CURRENT_USER){ alert("Set username first"); return; }
  fetch("/api/start", {method:"POST", headers:{"Content-Type":"application/json"},
    body: JSON.stringify({username: CURRENT_USER})
  }).then(r=>r.json()).then(d=>{
    alert(d.msg || (d.ok?"Started":"Failed"));
    document.getElementById('status').textContent = "Status: user=" + CURRENT_USER + (d.running?" (running)":" (stopped)");
  });
}

function stopConsumer(){
  fetch("/api/stop").then(r=>r.json()).then(d=>{
    alert(d.msg || "Stopped");
    document.getElementById('status').textContent = "Status: " + (CURRENT_USER?("user="+CURRENT_USER):"idle") + " (stopped)";
  });
}

function loadLogs(){
  if(!CURRENT_USER){ return; }
  fetch("/api/logs?username="+encodeURIComponent(CURRENT_USER))
    .then(r=>r.json()).then(d=>{
      const el = document.getElementById('logs');
      if(!d.ok){ el.textContent = "No logs"; return; }
      const lines = d.logs.map(x => `[${x.timestamp}] ${x.topic} → ${JSON.stringify(x.message)}`);
      const atBottom = (el.scrollTop + el.clientHeight + 10) >= el.scrollHeight;
      el.textContent = lines.join("\\n");
      if(atBottom){ el.scrollTop = el.scrollHeight; }
    });
}

setInterval(loadLogs, 1500);
setInterval(loadTopics, 3000);
</script>
</body>
</html>
"""

# ===================== ROUTES =====================
@app.get("/")
def home():
    return render_template_string(PAGE)

@app.post("/api/set_user")
def api_set_user():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    if not username:
        return {"ok": False, "msg": "Username required"}, 400
    RUN_STATE["username"] = username
    return {"ok": True, "running": RUN_STATE["running"]}

@app.get("/api/topics")
def api_topics():
    username = request.args.get("username", "").strip()
    if not username:
        return {"ok": False, "msg": "username required"}, 400

    approved = get_approved_topics()  # [(id,name)]
    subs = get_user_subscriptions(username)  # {topic_id}
    topics = [{"id": tid, "name": name, "subscribed": (tid in subs)} for (tid, name) in approved]
    return {"ok": True, "topics": topics}

@app.post("/api/subscribe")
def api_subscribe():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    topic_id = data.get("topic_id")
    if not username or not topic_id:
        return {"ok": False, "msg": "username and topic_id required"}, 400
    subscribe_topic(username, int(topic_id))
    return {"ok": True}

@app.post("/api/unsubscribe")
def api_unsubscribe():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    topic_id = data.get("topic_id")
    if not username or not topic_id:
        return {"ok": False, "msg": "username and topic_id required"}, 400
    unsubscribe_topic(username, int(topic_id))
    return {"ok": True}

@app.post("/api/start")
def api_start():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    if not username:
        return {"ok": False, "msg": "username required"}, 400

    RUN_STATE["username"] = username

    if RUN_STATE["running"]:
        return {"ok": False, "msg": "Already running", "running": True}, 400

    RUN_STATE["stop_event"].clear()
    RUN_STATE["running"] = True
    RUN_STATE["thread"] = threading.Thread(target=consumer_loop, daemon=True)
    RUN_STATE["thread"].start()
    return {"ok": True, "msg": "Consumer started", "running": True}

@app.get("/api/stop")
def api_stop():
    RUN_STATE["stop_event"].set()
    RUN_STATE["running"] = False
    return {"ok": True, "msg": "Consumer stop requested", "running": False}

@app.get("/api/logs")
def api_logs():
    username = request.args.get("username", "").strip()
    if not username:
        return {"ok": False, "msg": "username required"}, 400
    logs = get_recent_logs(username, limit_total=200)
    return {"ok": True, "logs": logs}

# ===================== MAIN =====================
if __name__ == "__main__":
    # Run the consumer UI for the user
    app.run(host="0.0.0.0", port=5002, debug=True)
