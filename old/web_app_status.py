from flask import Flask, request, redirect, url_for
import subprocess
import os
import ast  # Import to safely parse the list of dictionaries

app = Flask(__name__)

# Define the directory and script paths
#script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\JAN25"
script_dir = r"C:\Users\Administrator\Documents\algo\PythonAlgo\DEC24"
#login_script_name = "fully_auto_login_alice.py"
start_script_name = "output.py"

# Global variables
login_status_message = ""  # Global variable to hold login status message
login_status_color = ""  # Global variable to hold login status color
start_status_message = ""  # Global variable to hold start status message
start_status_color = ""  # Global variable to hold start status color
execution_data = []  # Global variable to store script execution data

@app.route("/")
def index():
    global login_status_message, login_status_color, start_status_message, start_status_color, execution_data
    # HTML to render the table for execution_data
    data_table = ""
    if execution_data:
        data_table = '''
        <table border="1" style="width:100%; border-collapse: collapse;">
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
        '''
        for row in execution_data:
            net_qty = int(row.get('Netqty', 0))
            mtom = float(row.get('MtoM', 0))
            
            # Determine the row style based on conditions
            row_style = ""
            if net_qty == 0 and mtom < 0:
                row_style = 'style="background-color: red;"'
            elif net_qty == 0 and mtom > 0:
                row_style = 'style="background-color: green;"'
            
            data_table += f'''
            <tr {row_style}>
                <td>{row.get('Symbol', '')}</td>
                <td>{row.get('Netqty', '')}</td>
                <td>{row.get('MtoM', '')}</td>
                <td>{row.get('LTP', '')}</td>
                <td>{row.get('Buyavgprc', '')}</td>
                <td>{row.get('Sellavgprc', '')}</td>
            </tr>
            '''
        data_table += '</tbody></table>'

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
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <h1> Redankaiah Uppala -- Algo Trade Setup </h1>
        
        <form action="/start" method="post">
            <button type="submit">START</button>
            <span class="status" style="color: {start_status_color};">{start_status_message}</span>
        </form>
        <h2>Todays Trades : </h2>
        {data_table}
    </body>
    </html>
    '''



@app.route("/start", methods=["POST"])
def execute_start_script():
    global start_status_message, start_status_color, execution_data
    try:
        # Change directory and execute the start script
        os.chdir(script_dir)
        result = subprocess.run(
            ["python", start_script_name],
            shell=True,
            text=True,
            capture_output=True
        )
        
        # Capture and preprocess script output
        raw_output = result.stdout.strip()
        print("Raw output from script:", raw_output)  # Debugging
        
        if raw_output.startswith("open Positions :"):
            raw_data = raw_output.split("open Positions :", 1)[1].strip()
            # Safely parse the string to Python object (list of dicts)
            execution_data = ast.literal_eval(raw_data)
        else:
            raise ValueError("Unexpected script output format.")
        
        start_status_message = "Script Executed Successfully"
        start_status_color = "green"
    except Exception as e:
        start_status_message = f"Error: {e}"
        start_status_color = "red"
        execution_data = []  # Clear data on error

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8088, debug=True)
