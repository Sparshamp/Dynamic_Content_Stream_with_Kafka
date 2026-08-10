from flask import Flask, render_template_string, request, jsonify
import mysql.connector, json

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": "172.29.25.56",
    "user": "team",
    "password": "team137",
    "database": "kafka_stream"
}

app = Flask(__name__, static_folder=None)

def db():
    return mysql.connector.connect(buffered=True,autocommit=True, **DB_CONFIG)

# ---------- HTML (single page) ----------
PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Admin Dashboard - Topics & Logs</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body{padding:18px;background:#f3f6fb}
    .card{border-radius:12px}
    .tab-btn{cursor:pointer}
    pre{white-space:pre-wrap;word-break:break-word}
  </style>
</head>
<body>
<div class="container">

  <nav class="navbar navbar-expand-lg mb-4" style="background:linear-gradient(90deg,#6753f6,#6ab7ff);border-radius:10px;padding:10px 18px;color:white">
    <a class="navbar-brand text-white" href="#"><strong>Admin Dashboard</strong></a>
    <div class="ms-auto">
      <button class="btn btn-outline-light btn-sm" onclick="loadAll()">Refresh</button>
    </div>
  </nav>

  <ul class="nav nav-tabs mb-3" id="myTabs">
    <li class="nav-item"><a class="nav-link active tab-btn" data-tab="topics" onclick="showTab('topics')">Topics</a></li>
    <li class="nav-item"><a class="nav-link tab-btn" data-tab="subs" onclick="showTab('subs')">Subscriptions</a></li>
    <li class="nav-item"><a class="nav-link tab-btn" data-tab="plogs" onclick="showTab('plogs')">Producer Logs</a></li>
    <li class="nav-item"><a class="nav-link tab-btn" data-tab="clogs" onclick="showTab('clogs')">Consumer Logs</a></li>
  </ul>

  <!-- Topics tab -->
  <div id="tab-topics">
    <div class="row g-3">
      <div class="col-md-6">
        <div class="card p-3">
          <h5>⏳ Pending Topics</h5>
          <div id="pendingList" class="mt-2"></div>
        </div>
      </div>

      <div class="col-md-6">
        <div class="card p-3">
          <h5>✅ Approved Topics</h5>
          <div id="approvedList" class="mt-2"></div>
        </div>
      </div>

      <div class="col-12 mt-3">
        <div class="card p-3">
          <h5>🟩 Active Topics</h5>
          <div id="activeList" class="mt-2"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Subscriptions tab -->
  <div id="tab-subs" style="display:none">
    <div class="card p-3">
      <h5>👥 User Subscriptions</h5>
      <div id="subsList" class="mt-2"></div>
    </div>
  </div>

  <!-- Producer logs -->
  <div id="tab-plogs" style="display:none">
    <div class="card p-3">
      <h5>📝 Producer Logs (latest)</h5>
      <div id="producerLogs" class="mt-2"></div>
    </div>
  </div>

  <!-- Consumer logs -->
  <div id="tab-clogs" style="display:none">
    <div class="card p-3">
      <h5>📥 Consumer Logs (latest)</h5>
      <div id="consumerLogs" class="mt-2"></div>
    </div>
  </div>

</div>

<script>
function showTab(name){
  ["topics","subs","plogs","clogs"].forEach(t=>{
    document.getElementById("tab-"+t).style.display=(t===name?"block":"none");
    document.querySelectorAll('.nav-link').forEach(n=>n.classList.remove('active'));
  });
  document.querySelector('[data-tab="'+name+'"]').classList.add('active');
  if(name==="topics") loadTopics();
  if(name==="subs") loadSubs();
  if(name==="plogs") loadProducerLogs();
  if(name==="clogs") loadConsumerLogs();
}

function fetchJSON(url, opts){
  return fetch(url, opts).then(r=>{
    if(!r.ok) throw new Error('HTTP error ' + r.status);
    return r.json();
  });
}

function loadAll(){ loadTopics(); loadSubs(); loadProducerLogs(); loadConsumerLogs(); }

function loadTopics(){
  fetchJSON('/api/topics').then(d=>{
    if(!d.ok) return alert("Failed to load topics");
    const pending = d.pending || [];
    const approved = d.approved || [];
    const active = d.active || [];

    const pendBox = document.getElementById('pendingList');
    pendBox.innerHTML = pending.map(t=>`<div class="d-flex justify-content-between align-items-center border rounded p-2 mb-2">
      <div><strong>${t.name}</strong> <small class="text-muted">#${t.id}</small></div>
      <div>
        <button class="btn btn-sm btn-success me-2" onclick="approveTopic(${t.id})">Approve</button>
        <button class="btn btn-sm btn-danger" onclick="rejectTopic(${t.id})">Reject</button>
      </div>
    </div>`).join('') || '<div class="text-muted">No pending topics</div>';

    const apprBox = document.getElementById('approvedList');
    apprBox.innerHTML = approved.map(t=>`<div class="d-flex justify-content-between align-items-center border rounded p-2 mb-2">
      <div><strong>${t.name}</strong> <small class="text-muted">#${t.id}</small></div>
      <div>
        <button class="btn btn-sm btn-danger" onclick="rejectTopic(${t.id})">Reject</button>
      </div>
    </div>`).join('') || '<div class="text-muted">No approved topics</div>';

    const actBox = document.getElementById('activeList');
    actBox.innerHTML = active.map(t=>`<div class="d-flex justify-content-between align-items-center border rounded p-2 mb-2">
      <div>
        <strong>${t.name}</strong> <small class="text-muted">#${t.id}</small>
        <div class="small text-muted">${t.subscribers} subscribers</div>
      </div>
      <div>
        <button class="btn btn-sm btn-warning me-2" onclick="deactivateTopic(${t.id})">Deactivate</button>
        <button class="btn btn-sm btn-danger" onclick="rejectTopic(${t.id})">Reject</button>
      </div>
    </div>`).join('') || '<div class="text-muted">No active topics</div>';
  }).catch(err=>{
    console.error("Error loading topics:", err);
    alert("Error loading topics: " + err.message);
  });
}

function approveTopic(id){
  fetchJSON('/api/approve', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})})
    .then(d=>{ alert(d.msg); loadTopics(); })
    .catch(err=>alert("Error: " + err.message));
}

function activateTopic(id){
  fetchJSON('/api/activate', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})})
    .then(d=>{ alert(d.msg); loadTopics(); })
    .catch(err=>alert("Error: " + err.message));
}

function deactivateTopic(id){
  fetchJSON('/api/deactivate', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})})
    .then(d=>{ alert(d.msg); loadTopics(); })
    .catch(err=>alert("Error: " + err.message));
}

function rejectTopic(id){
  if(!confirm("Delete this topic permanently? This will also delete all subscriptions to this topic.")) return;
  
  console.log("🗑️ Attempting to delete topic ID:", id);
  fetchJSON('/api/reject', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({id})})
    .then(d=>{
      console.log("✅ Delete response:", d);
      alert(d.msg);
      loadTopics();
      loadSubs(); // Refresh subscriptions too since they may have been deleted
    })
    .catch(err=>{
      console.error("❌ Error deleting topic:", err);
      alert("Error deleting topic: " + err.message);
    });
}

function loadSubs(){
  fetchJSON('/api/subscriptions').then(d=>{
    if(!d.ok) return alert("Failed to load subscriptions");
    const box = document.getElementById('subsList');
    box.innerHTML = d.rows.map(r=>`<div class="d-flex justify-content-between align-items-center border rounded p-2 mb-2">
      <div><strong>${r.user}</strong> → <code>${r.topic}</code> (topic_id=${r.topic_id})</div>
      <div>
        <button class="btn btn-sm btn-danger" onclick="deleteSubscription(${r.id})">UnSubscibe</button>
      </div>
    </div>`).join('') || '<div class="text-muted">No subscriptions</div>';
  }).catch(err=>{
    console.error("Error loading subscriptions:", err);
    alert("Error loading subscriptions: " + err.message);
  });
}

function deleteSubscription(id){
  console.log("🟠 Delete button clicked for subscription ID:", id);
  if(!confirm("Remove this subscription?")) return;

  console.log("📡 Sending API request → /api/delete_subscription with id:", id);
  fetchJSON('/api/delete_subscription', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id: id})
  })
  .then(d=>{
    console.log("✅ Response from API:", d);
    alert(d.msg || "Subscription deleted successfully");
    loadSubs();
  })
  .catch(err=>{
    console.error("❌ API call failed:", err);
    alert("Error deleting subscription: " + err.message);
  });
}

function loadProducerLogs(){
  fetchJSON('/api/producer_logs').then(d=>{
    const el = document.getElementById('producerLogs');
    if(!d.ok) return el.innerHTML = '<div class="text-muted">No logs</div>';
    el.innerHTML = d.rows.map(r=>`<div class="border rounded p-2 mb-2">
      <div class="small text-muted">${r.timestamp}</div>
      <div><strong>${r.topic_name}</strong></div>
      <pre>${JSON.stringify(r.message, null, 2)}</pre>
    </div>`).join('') || '<div class="text-muted">No logs</div>';
  }).catch(err=>console.error("Error loading producer logs:", err));
}

function loadConsumerLogs(){
  fetchJSON('/api/consumer_logs').then(d=>{
    const el = document.getElementById('consumerLogs');
    if(!d.ok) return el.innerHTML = '<div class="text-muted">No logs</div>';
    el.innerHTML = d.rows.map(r=>`<div class="border rounded p-2 mb-2">
      <div class="small text-muted">${r.timestamp} • ${r.username}</div>
      <div><strong>${r.topic_name}</strong></div>
      <pre>${JSON.stringify(r.message, null, 2)}</pre>
    </div>`).join('') || '<div class="text-muted">No logs</div>';
  }).catch(err=>console.error("Error loading consumer logs:", err));
}

// initial load
loadTopics();
</script>

</body>
</html>
"""

# ---------- API ROUTES ----------
@app.get("/")
def home():
    return render_template_string(PAGE)

@app.get("/api/topics")
def api_topics():
    try:
        con = db()
        cur = con.cursor(buffered=True)

        # Pending topics
        cur.execute("SELECT id, name FROM topics WHERE status='pending' ORDER BY id DESC")
        pending = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]

        # Approved topics (not yet active)
        cur.execute("SELECT id, name FROM topics WHERE status='approved' ORDER BY id DESC")
        approved = [{"id": r[0], "name": r[1]} for r in cur.fetchall()]

        # Active topics
        cur.execute("SELECT id, name FROM topics WHERE status='active' ORDER BY id DESC")
        active_rows = cur.fetchall()
        active = []
        for r in active_rows:
            tid, name = r[0], r[1]
            cur2 = con.cursor(buffered=True)
            cur2.execute("SELECT COUNT(*) FROM user_subscriptions WHERE topic_id=%s", (tid,))
            subs = cur2.fetchone()[0]
            cur2.close()
            active.append({"id": tid, "name": name, "subscribers": subs})

        cur.close()
        con.close()
        return {"ok": True, "pending": pending, "approved": approved, "active": active}
    except Exception as e:
        return {"ok": False, "msg": str(e)}, 500


@app.post("/api/approve")
def api_approve():
    data = request.get_json(silent=True) or {}
    tid = data.get("id")
    if not tid:
        return {"ok": False, "msg": "id required"}, 400
    try:
        con = db()
        cur = con.cursor()
        cur.execute("UPDATE topics SET status='approved' WHERE id=%s", (int(tid),))
        con.commit()
        cur.close()
        con.close()
        return {"ok": True, "msg": "Topic approved"}
    except Exception as e:
        return {"ok": False, "msg": f"Error: {str(e)}"}, 500

@app.post("/api/activate")
def api_activate():
    return api_approve()

@app.post("/api/deactivate")
def api_deactivate():
    data = request.get_json(silent=True) or {}
    tid = data.get("id")
    if not tid: 
        return {"ok": False, "msg": "id required"}, 400
    try:
        con = db()
        cur = con.cursor()
        cur.execute("UPDATE topics SET status='approved' WHERE id=%s", (int(tid),))
        con.commit()
        cur.close()
        con.close()
        return {"ok": True, "msg": "Topic deactivated"}
    except Exception as e:
        return {"ok": False, "msg": f"Error: {str(e)}"}, 500

@app.post("/api/reject")
def api_reject():
    data = request.get_json(silent=True) or {}
    tid = data.get("id")
    if not tid:
        return {"ok": False, "msg": "id required"}, 400
    
    try:
        con = db()
        cur = con.cursor(buffered=True)
        
        # First, manually delete all subscriptions for this topic
        print(f"🗑️ Deleting subscriptions for topic_id={tid}")
        cur.execute("DELETE FROM user_subscriptions WHERE topic_id=%s", (int(tid),))
        deleted_subs = cur.rowcount
        print(f"✅ Deleted {deleted_subs} subscriptions")
        
        # Then delete the topic
        print(f"🗑️ Deleting topic id={tid}")
        cur.execute("DELETE FROM topics WHERE id=%s", (int(tid),))
        deleted_topics = cur.rowcount
        print(f"✅ Deleted {deleted_topics} topic(s)")
        
        con.commit()
        cur.close()
        con.close()
        
        return {"ok": True, "msg": f"Topic deleted ({deleted_subs} subscriptions removed)"}
    except Exception as e:
        print(f"❌ Error in api_reject: {str(e)}")
        return {"ok": False, "msg": f"Error deleting topic: {str(e)}"}, 500

@app.get("/api/subscriptions")
def api_subs():
    try:
        con = db()
        cur = con.cursor(buffered=True)
        cur.execute("""
          SELECT us.id, us.`user`, us.topic_id, t.name
          FROM user_subscriptions us
          JOIN topics t ON t.id = us.topic_id
          ORDER BY us.`user`, t.name
        """)
        rows = [{"id":r[0],"user":r[1],"topic_id":r[2],"topic":r[3]} for r in cur.fetchall()]
        cur.close()
        con.close()
        return {"ok": True, "rows": rows}
    except Exception as e:
        return {"ok": False, "msg": str(e)}, 500

@app.get("/api/producer_logs")
def api_producer_logs():
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT id, topic_name, message, timestamp FROM producer_logs ORDER BY id DESC LIMIT 200")
        rows = []
        for r in cur.fetchall():
            try:
                msg = json.loads(r[2]) if isinstance(r[2], str) else r[2]
            except:
                msg = r[2]
            rows.append({"id": r[0], "topic_name": r[1], "message": msg, "timestamp": r[3].strftime('%Y-%m-%d %H:%M:%S')})
        cur.close()
        con.close()
        return {"ok": True, "rows": rows}
    except Exception as e:
        return {"ok": False, "msg": str(e)}, 500

@app.get("/api/consumer_logs")
def api_consumer_logs():
    try:
        con = db()
        cur = con.cursor()
        cur.execute("SELECT id, username, topic_name, message, timestamp FROM consumer_logs ORDER BY id DESC LIMIT 200")
        rows = []
        for r in cur.fetchall():
            try:
                msg = json.loads(r[3]) if isinstance(r[3], str) else r[3]
            except:
                msg = r[3]
            rows.append({"id": r[0], "username": r[1], "topic_name": r[2], "message": msg, "timestamp": r[4].strftime('%Y-%m-%d %H:%M:%S')})
        cur.close()
        con.close()
        return {"ok": True, "rows": rows}
    except Exception as e:
        return {"ok": False, "msg": str(e)}, 500
    
@app.post("/api/delete_subscription")
def api_delete_subscription():
    data = request.get_json(silent=True) or {}
    sid = data.get("id")
    
    if not sid:
        return {"ok": False, "msg": "subscription id required"}, 400

    try:
        con = db()
        cur = con.cursor(buffered=True)
        
        print(f"🗑️ Attempting to delete subscription id={sid}")
        cur.execute("DELETE FROM user_subscriptions WHERE id = %s", (int(sid),))
        deleted = cur.rowcount
        print(f"✅ Rows affected: {deleted}")
        
        con.commit()
        cur.close()
        con.close()
        
        if deleted > 0:
            return {"ok": True, "msg": f"Subscription {sid} deleted successfully"}
        else:
            return {"ok": False, "msg": f"Subscription {sid} not found"}, 404
            
    except Exception as e:
        print(f"❌ Error in api_delete_subscription: {str(e)}")
        return {"ok": False, "msg": f"Error deleting subscription: {str(e)}"}, 500


# ---------- RUN ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003, debug=True)
