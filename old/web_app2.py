from flask import Flask, render_template, request
import subprocess
import os

app = Flask(__name__)

# Define the directory and script paths
script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
login_script_name = "fully_auto_login_alice.py"
start_script_name = "dec18_thr_Print3_test.py"

@app.route("/")
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Run Python Script</title>
    </head>
    <body>
        <h1>Run Python Script</h1>
        <form action="/login" method="post">
            <button type="submit">LOGIN</button>
        </form>
        <p id="login_status"></p>
        <form action="/start" method="post">
            <button type="submit">START</button>
        </form>
    </body>
    </html>
    '''

@app.route("/login", methods=["POST"])
def execute_login_script():
    try:
        # Change directory and execute the login script
        os.chdir(script_dir)
        result = subprocess.run(["python3", login_script_name], shell=True, text=True, capture_output=True)
        if "Logged in successfully" in result.stdout:
            return '''
            <!DOCTYPE html>
            <html>
            <body>
                <h1>Login Successful</h1>
                <p>Logged in successfully, welcome REDANKAIAH UPPALA</p>
                <a href="/">Go back</a>
            </body>
            </html>
            '''
        else:
            return '''
            <!DOCTYPE html>
            <html>
            <body>
                <h1>Login Failed</h1>
                <p>Please try again.</p>
                <a href="/">Go back</a>
            </body>
            </html>
            '''
    except Exception as e:
        return f"Error: {e}. Go back to <a href='/'>home</a>."

@app.route("/start", methods=["POST"])
def execute_start_script():
    try:
        # Change directory and execute the start script
        os.chdir(script_dir)
        subprocess.Popen(["python3", start_script_name], shell=True)
        return "Script started successfully! Go back to <a href='/'>home</a>."
    except Exception as e:
        return f"Error: {e}. Go back to <a href='/'>home</a>."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
