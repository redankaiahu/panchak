from flask import Flask, request, redirect, url_for
import subprocess
import os

app = Flask(__name__)

# Define the directory and script paths
script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
login_script_name = "fully_auto_login_alice.py"
start_script_name = "dec18_five_thr_two_TEST4.py"

login_status_message = ""  # Global variable to hold login status message
login_status_color = ""  # Global variable to hold login status color

start_status_message = ""  # Global variable to hold start status message
start_status_color = ""  # Global variable to hold start status color

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
        </style>
    </head>
    <body>
        <h1>Run Python Script</h1>
        <form action="/login" method="post">
            <button type="submit">LOGIN</button>
            <span class="status" style="color: {login_status_color};">{login_status_message}</span>
        </form>
        <p id="login_status"></p>
        <form action="/start" method="post">
            <button type="submit">START</button>
            <span class="status" style="color: {start_status_color};">{start_status_message}</span>
        </form>
    </body>
    </html>
    '''

@app.route("/login", methods=["POST"])
def execute_login_script():
    global login_status_message, login_status_color
    try:
        # Change directory and execute the login script
        os.chdir(script_dir)
        result = subprocess.run(
            ["python3", login_script_name],
            shell=True,
            text=True,
            capture_output=True
        )

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

    return redirect(url_for("index"))

@app.route("/start", methods=["POST"])
def execute_start_script():
    global start_status_message, start_status_color
    try:
        # Change directory and execute the start script
        os.chdir(script_dir)
        subprocess.Popen(["python3", start_script_name], shell=True)
        start_status_message = "Script Started Successfully"
        start_status_color = "green"
    except Exception as e:
        start_status_message = f"Error: {e}"
        start_status_color = "red"

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
