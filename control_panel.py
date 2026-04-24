from flask import Flask, request, render_template_string
import subprocess
import os
import signal
import sys

app = Flask(__name__)

KITE_LOGIN_URL = "https://kite.zerodha.com/connect/login?api_key=7am67kxijfsusk9i"
TOKEN_FILE = "access_token_test.txt"
PID_FILE = "dashboard.pid"
DASHBOARD_CMD = [
    sys.executable, "-m", "streamlit", "run",
    "panchak_kite_dashboard24_test.py",
    "--server.port=8089",
    "--server.address=0.0.0.0"
]


HTML = """
<h2>🧠 Algo Control Panel</h2>

<p>
<a href="{{ kite_url }}" target="_blank">🔑 Kite Login (Get Access Token)</a>
</p>

<form method="post">
    <label>Paste Access Token:</label><br>
    <input type="text" name="token" style="width:400px"><br><br>
    <button name="action" value="save">💾 Save Token</button>
</form>

<hr>

<form method="post">
    <button name="action" value="start">▶️ Start Dashboard</button>
    <button name="action" value="stop">⏹ Stop Dashboard</button>
</form>

<p>{{ message }}</p>

<p>
📊 Dashboard URL:
<a href="http://{{ host }}:8089" target="_blank">
http://{{ host }}:8089
</a>
</p>
"""

@app.route("/", methods=["GET", "POST"])
def index():
    message = ""
    if request.method == "POST":
        action = request.form.get("action")

        if action == "save":
            token = request.form.get("token")
            if token:
                with open(TOKEN_FILE, "w") as f:
                    f.write(token.strip())
                message = "✅ Access token saved"
            else:
                message = "❌ Token is empty"

        elif action == "start":
            if os.path.exists(PID_FILE):
                message = "⚠️ Dashboard already running"
            else:
                p = subprocess.Popen(DASHBOARD_CMD)
                with open(PID_FILE, "w") as f:
                    f.write(str(p.pid))
                message = "▶️ Dashboard started"

        elif action == "stop":
            if os.path.exists(PID_FILE):
                pid = int(open(PID_FILE).read())
                os.kill(pid, signal.SIGTERM)
                os.remove(PID_FILE)
                message = "⏹ Dashboard stopped"
            else:
                message = "⚠️ Dashboard not running"

    return render_template_string(
        HTML,
        kite_url=KITE_LOGIN_URL,
        message=message,
        host=request.host.split(":")[0]
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
