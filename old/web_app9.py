from flask import Flask, request, redirect, url_for
import subprocess
import os
import signal
import logging
import sys
from datetime import datetime

app = Flask(__name__)

# Define the directory and script paths
script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
login_script_name = "fully_auto_login_alice.py"
start_script_name = "dec18_five_thr_two_TEST2_1.py"

# Add separate variables for login and start statuses
login_status_message = ""
login_status_color = ""
start_status_message = ""
start_status_color = ""
processes = []  # Track started processes for termination

# Logging setup
log_file = f"web_app_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to log to both terminal and file
def log_message(level, message):
    print(message)
    logging.log(level, message)

@app.route("/")
def index():
    global login_status_message, login_status_color, start_status_message, start_status_color
    return f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Run Python Script</title>
        <style>
            .status {{
                margin-left: 10px;
                font-weight: bold;
            }}
            .button-container {{
                margin: 15px 0;
            }}
            button {{
                padding: 10px 20px;
                font-size: 16px;
            }}
        </style>
    </head>
    <body>
        <h1>Run Python Script</h1>
        <div class="button-container">
            <form action="/login" method="post">
                <button type="submit">LOGIN</button>
                <span class="status" style="color: {login_status_color};">{login_status_message}</span>
            </form>
        </div>
        <div class="button-container">
            <form action="/start" method="post">
                <button type="submit">START</button>
                <span class="status" style="color: {start_status_color};">{start_status_message}</span>
            </form>
        </div>
        <div class="button-container">
            <form action="/exit" method="post">
                <button type="submit">EXIT</button>
            </form>
        </div>
    </body>
    </html>
    '''

@app.route("/login", methods=["POST"])
def execute_login_script():
    global login_status_message, login_status_color
    try:
        os.chdir(script_dir)
        result = subprocess.run(
            ["python", login_script_name],
            shell=True,
            text=True,
            capture_output=True
        )
        log_message(logging.INFO, f"Login script output: {result.stdout}")
        log_message(logging.ERROR, f"Login script error: {result.stderr}")
        # Debugging output
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print("Return Code:", result.returncode)

        if result.returncode == 0 and "Logged in successufly" in result.stdout:
            login_status_message = "Login Successful"
            login_status_color = "green"
        else:
            login_status_message = "Login Failed"
            login_status_color = "red"
    except Exception as e:
        login_status_message = f"Error: {e}"
        login_status_color = "red"
        log_message(logging.ERROR, f"Error in login: {e}")
    return redirect(url_for("index"))

@app.route("/start", methods=["POST"])
def execute_start_script():
    global start_status_message, start_status_color
    try:
        os.chdir(script_dir)
        process = subprocess.Popen(["python", start_script_name], shell=True)
        processes.append(process)
        start_status_message = "Script Started Successfully"
        start_status_color = "green"
        log_message(logging.INFO, f"Started script {start_script_name}.")
    except Exception as e:
        start_status_message = f"Error: {e}"
        start_status_color = "red"
        log_message(logging.ERROR, f"Error starting script: {e}")
    return redirect(url_for("index"))

@app.route("/exit", methods=["POST"])
def exit_all():
    global login_status_message, login_status_color, start_status_message, start_status_color
    try:
        # Terminate all running processes
        for process in processes:
            os.kill(process.pid, signal.SIGTERM)
        processes.clear()
        log_message(logging.INFO, "All running scripts terminated.")
        
        # Reset statuses
        login_status_message = ""
        login_status_color = ""
        start_status_message = ""
        start_status_color = ""

        # Restart the Flask app by redirecting to the home page
        return redirect(url_for("index"))

    except Exception as e:
        log_message(logging.ERROR, f"Error during exit: {e}")
        return f"Error during exit: {e}", 500

if __name__ == "__main__":
    # Ensure debug reloader does not interfere
    os.environ["FLASK_RUN_FROM_CLI"] = "false"

    # Custom startup message
    log_message(logging.INFO, "Starting Flask Web App...")
    print("Starting Flask Web App...")

    # Run Flask application
    app.run(host="0.0.0.0", port=8080, debug=True, use_reloader=False)
