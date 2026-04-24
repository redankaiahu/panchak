from flask import Flask, render_template_string, send_file, request
import os
import subprocess
import pandas as pd
import ast
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)

script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\2025"
start_script_name = "output.py"

# Folder to store daily trade files
TRADE_FOLDER = "trades"
os.makedirs(TRADE_FOLDER, exist_ok=True)

execution_data = {}

# Load existing trade data from Excel files on startup
def load_existing_trade_data():
    global execution_data
    files = sorted(os.listdir(TRADE_FOLDER), reverse=True)
    for file in files:
        if file.startswith("trades_") and file.endswith(".xlsx"):
            date_str = file.replace("trades_", "").replace(".xlsx", "")
            file_path = os.path.join(TRADE_FOLDER, file)
            df = pd.read_excel(file_path)
            execution_data[date_str] = df.to_dict(orient='records')

# Function to fetch available trade dates
def get_available_trade_dates():
    return sorted(execution_data.keys(), reverse=True)

def get_mtom_for_date(date):
    if date in execution_data:
        return sum(float(row.get('MtoM', 0)) for row in execution_data[date])
    return 0

def run_script_periodically():
    while True:
        execute_start_script()
        save_trades_to_excel()
        time.sleep(100)

def execute_start_script():
    global execution_data
    try:
        os.chdir(script_dir)
        result = subprocess.run(
            ["python", start_script_name], shell=True, text=True, capture_output=True
        )
        raw_output = result.stdout.strip()
        
        if raw_output.startswith("open Positions :"):
            raw_data = raw_output.split("open Positions :", 1)[1].strip()
            execution_data[datetime.now().strftime("%Y-%m-%d")] = ast.literal_eval(raw_data)
    except Exception as e:
        execution_data[datetime.now().strftime("%Y-%m-%d")] = []

def save_trades_to_excel():
    today_date = datetime.now().strftime("%Y-%m-%d")
    if today_date not in execution_data or not execution_data[today_date]:
        return
    file_path = os.path.join(TRADE_FOLDER, f"trades_{today_date}.xlsx")
    df = pd.DataFrame(execution_data[today_date])
    df.to_excel(file_path, index=False)
    print(f"Trades saved: {file_path}")

@app.route("/")
def index():
    execute_start_script()
    save_trades_to_excel()
    total_mtom = sum(float(row.get('MtoM', 0)) for row in execution_data.get(datetime.now().strftime("%Y-%m-%d"), []))
    mtom_color = "green" if total_mtom > 0 else "red" if total_mtom < 0 else "black"
    current_date = datetime.now().strftime("%d %b %Y")

    html_template = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Algo Trade Status</title>
        <style>
            .top-right {{ position: absolute; top: 10px; right: 10px; font-size: 18px; }}
            .daily-trades-btn {{ position: absolute; top: 10px; right: 150px; font-size: 18px; background-color: green; color: white; padding: 15px; border: none; cursor: pointer; }}
            .trade-table {{ width: 100%; border-collapse: collapse; }}
            .trade-table th, .trade-table td {{ border: 1px solid black; padding: 8px; text-align: center; }}
            .trade-table th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h1>Uppala Redankaiah Trade Setup</h1>
        <h2>Today's Trades - {current_date}</h2>
        <h2>MtoM: <span style="color: {mtom_color};">{total_mtom:.2f}</span></h2>
        <button class="daily-trades-btn" onclick="window.location.href='/daily-trades'">Daily Trades</button>
        <table class="trade-table">
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Net Quantity</th>
                    <th>MtoM</th>
                    <th>LTP</th>
                    <th>Buy Avg Price</th>
                    <th>Sell Avg Price</th>
                </tr>
            </thead>
            <tbody>
                {"".join([
                    #f"<tr><td>{row.get('Symbol', '')}</td><td>{row.get('Netqty', '')}</td><td style='color:{'green' if float(row.get('MtoM', 0)) > 0 else 'red'}'>{row.get('MtoM', '')}</td>"
                    f"<tr><td>{row.get('Symbol', '')}</td><td>{row.get('Netqty', '')}</td><td style='color:white;background-color: {'green' if float(row.get('MtoM', 0)) > 0 else 'red'};'>{row.get('MtoM', '')}</td>"
                    f"<td>{row.get('LTP', '')}</td><td>{row.get('Buyavgprc', '')}</td><td>{row.get('Sellavgprc', '')}</td></tr>"
                    for row in execution_data.get(datetime.now().strftime("%Y-%m-%d"), [])
                ])}
            </tbody>
        </table>
    </body>
    </html>
    """
    return render_template_string(html_template)

@app.route("/daily-trades")
def daily_trades():
    trade_dates = get_available_trade_dates()
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Daily Trades</title>
        <style>
            .top-right { position: absolute; top: 10px; right: 10px; font-size: 18px; }
            .trade-button { background-color: blue; color: white; padding: 15px; border: none; cursor: pointer; font-size: 16px; margin: 10px; }
            .mtom-value { font-size: 18px; margin-left: 20px; font-weight: bold; }
        </style>
        <script>
            function loadTrades(date) { window.location.href = "/trades/" + date; }
        </script>
    </head>
    <body>
        <h1>Daily Trades</h1>
        <a href="/" class="top-right">← Back to Menu</a><br>
        {% for date in trade_dates %}
            <div style="display: flex; align-items: center; margin-bottom: 10px;">
                <button class="trade-button" onclick="loadTrades('{{ date }}')">{{ date }}</button>
                <span class="mtom-value" style="color: {% if get_mtom_for_date(date) < 0 %}red{% else %}green{% endif %};">
                    {{ '{:.2f}'.format(get_mtom_for_date(date)) }}
                </span>
            </div>
        {% endfor %}
    </body>
    </html>
    """
    return render_template_string(html_template, trade_dates=trade_dates, get_mtom_for_date=get_mtom_for_date)

@app.route("/trades/<date>")
def show_trade_data(date):
    file_path = os.path.join(TRADE_FOLDER, f"trades_{date}.xlsx")
    if not os.path.exists(file_path):
        return "<h1>No data available for this date.</h1><br><a href='/daily-trades'>← Back to Daily Trades</a>"
    df = pd.read_excel(file_path)
    df = df[['Symbol', 'MtoM', 'LTP', 'Buyavgprc', 'Sellavgprc']]
    trade_table = df.to_html(classes="trade-table", index=False)
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Trades for {date}</title>
        <style>
            .spacer {{ margin-top: 20px; }}
        </style>
    </head>
    <body>
        <h1>Trade Data for {date}</h1>
        <a href='/daily-trades'>← Back to Daily Trades</a>

        <div class="spacer"></div>
        {trade_table}
    </body>
    </html>
    """



if __name__ == "__main__":
    load_existing_trade_data()
    print("Routes available:", app.url_map)  # Debug log to check registered routes
    threading.Thread(target=run_script_periodically, daemon=True).start()
    app.run(host="0.0.0.0", port=8088, debug=True)
