import logging
import os
import subprocess
import sys
import signal
from datetime import datetime
from flask import Flask, request, redirect, url_for

app = Flask(__name__)

# Define the directory and script paths
script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
login_script_name = "fully_auto_login_alice.py"
start_script_name = "feb2025_v8_two.py"
BUY_script_name = "Buy_Feb25.py"
SELL_script_name = "Sell_Feb25.py"

# Add separate variables for login and start statuses
login_status_message = ""
login_status_color = ""
start_status_message = ""
start_status_color = ""
buy_status_message = ""
buy_status_color = ""
sell_status_message = ""
sell_status_color = ""

processes = []  # Track started processes for termination

# Logging setup
log_file = f"web_app_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

sys.stdout = sys.stderr = open(log_file, "a")

@app.route("/")
def index():
    global login_status_message, login_status_color, start_status_message, start_status_color, buy_status_message, buy_status_color, sell_status_message, sell_status_color
    return f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Run Python Script</title>
        <style>
            .status {{ margin-left: 10px; font-weight: bold; }}
            .button-container {{ margin: 15px 0; }}
            button {{ padding: 10px 20px; font-size: 16px; }}
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
            <form action="/BUY" method="post">
                <button type="submit">BUY</button>
                <span class="status" style="color: {buy_status_color};">{buy_status_message}</span>
            </form>
        </div>
        <div class="button-container">
            <form action="/SELL" method="post">
                <button type="submit">SELL</button>
                <span class="status" style="color: {sell_status_color};">{sell_status_message}</span>
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

@app.route("/start", methods=["POST"])
def execute_start_script():
    global start_status_message, start_status_color
    try:
        os.chdir(script_dir)
        process = subprocess.Popen(["python", start_script_name], shell=True)
        processes.append(process)
        start_status_message = " script Started Successfully"
        start_status_color = "green"
        logger.info(f"Started script {start_script_name}.")
    except Exception as e:
        logger.error(f"Error starting script: {e}")
        start_status_message = f"Error: {e}"
        start_status_color = "red"
    return redirect(url_for("index"))

@app.route("/BUY", methods=["POST"])
def execute_buy_script():
    global buy_status_message, buy_status_color
    try:
        os.chdir(script_dir)
        process = subprocess.Popen(["python", BUY_script_name], shell=True)
        processes.append(process)
        buy_status_message = "BUY script Started Successfully"
        buy_status_color = "green"
        logger.info(f"Started script {BUY_script_name}.")
    except Exception as e:
        logger.error(f"Error starting script: {e}")
        buy_status_message = f"Error: {e}"
        buy_status_color = "red"
    return redirect(url_for("index"))

@app.route("/SELL", methods=["POST"])
def execute_sell_script():
    global sell_status_message, sell_status_color
    try:
        os.chdir(script_dir)
        process = subprocess.Popen(["python", SELL_script_name], shell=True)
        processes.append(process)
        sell_status_message = "SELL script Started Successfully"
        sell_status_color = "green"
        logger.info(f"Started script {SELL_script_name}.")
    except Exception as e:
        logger.error(f"Error starting script: {e}")
        sell_status_message = f"Error: {e}"
        sell_status_color = "red"
    return redirect(url_for("index"))

@app.route("/exit", methods=["POST"])
def exit_all():
    global login_status_message, login_status_color, start_status_message, start_status_color, buy_status_message, buy_status_color, sell_status_message, sell_status_color
    try:
        for process in processes:
            os.kill(process.pid, signal.SIGTERM)
        processes.clear()
        logger.info("All running scripts terminated.")

        # Reset statuses
        login_status_message = ""
        login_status_color = ""
        start_status_message = ""
        start_status_color = ""

        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error during exit: {e}")
        return f"Error during exit: {e}", 500

if __name__ == "__main__":
    logger.info("Starting Flask Web App...")
    app.run(host="0.0.0.0", port=8080, debug=True, use_reloader=False)