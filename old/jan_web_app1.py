#pip install psutil


from flask import Flask, request, redirect, url_for
import subprocess
import os
import psutil


app = Flask(__name__)

# Define the directory and script paths
script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
login_script_name = "fully_auto_login_alice.py"
start_script_name = "HistoricalTest6_buyandsell.py"

login_status_message = ""  # Global variable to hold login status message
login_status_color = ""  # Global variable to hold login status color

start_status_message = ""  # Global variable to hold start status message
start_status_color = ""  # Global variable to hold start status color

exit_status_message = ""  # Global variable to hold exit status message
exit_status_color = ""  # Global variable to hold exit status color

running_processes = []  # List to track running processes

@app.route("/")
def index():
    global login_status_message, login_status_color, start_status_message, start_status_color, exit_status_message, exit_status_color
    return f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Run Python Script</title>
        <style>
            .status {{
                margin-left: 15px;
                font-weight: bold;
            }}
            form {{
                margin-bottom: 50px;
            }}
            button {{
                padding: 15px 30px;
                font-size: 16px;
            }}
        </style>
    </head>
    <body>
        <h1>Run Python Script</h1>
        <form action="/login" method="post">
            <button type="submit">LOGIN</button>
            <span class="status" style="color: {login_status_color};">{login_status_message}</span>
        </form>
        <form action="/start" method="post">
            <button type="submit">START</button>
            <span class="status" style="color: {start_status_color};">{start_status_message}</span>
        </form>
        <form action="/exit" method="post">
            <button type="submit">EXIT</button>
            <span class="status" style="color: {exit_status_color};">{exit_status_message}</span>
        </form>
    </body>
    </html>
    '''

@app.route("/login", methods=["POST"])
def execute_login_script():
    global login_status_message, login_status_color
    try:
        os.chdir(script_dir)
        result = subprocess.run(
            ["python3", login_script_name],
            shell=True,
            text=True,
            capture_output=True
        )

        if result.returncode == 0 and "Logged in successfully" in result.stdout:
            login_status_message = "Login Successful"
            login_status_color = "green"
        else:
            login_status_message = "Login Failed"
            login_status_color = "red"

    except Exception as e:
        login_status_message = f"Error: {e}"
        login_status_color = "red"

    return redirect(url_for("index"))

@app.route("/start", methods=["POST"])
def execute_start_script():
    global start_status_message, start_status_color, running_processes
    try:
        os.chdir(script_dir)
        process = subprocess.Popen(["python3", start_script_name], shell=True)
        running_processes.append(process.pid)
        start_status_message = "Script Started Successfully"
        start_status_color = "green"
    except Exception as e:
        start_status_message = f"Error: {e}"
        start_status_color = "red"

    return redirect(url_for("index"))

@app.route("/exit", methods=["POST"])
def exit_scripts():
    global exit_status_message, exit_status_color, running_processes
    try:
        for pid in running_processes:
            if psutil.pid_exists(pid):
                parent = psutil.Process(pid)
                for child in parent.children(recursive=True):
                    child.kill()
                parent.kill()
        running_processes.clear()
        exit_status_message = "All scripts terminated successfully"
        exit_status_color = "green"
    except Exception as e:
        exit_status_message = f"Error: {e}"
        exit_status_color = "red"

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
