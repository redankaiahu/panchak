from flask import Flask, render_template, request
import subprocess
import os

app = Flask(__name__)

# Define the directory and script path
script_dir = r"C:\Users\aarya\OneDrive\Documents\PythonAlgo\DEC24"
script_name = "dec18_thr_Print3_test.py"

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
        <form action="/start" method="post">
            <button type="submit">Start</button>
        </form>
    </body>
    </html>
    '''

@app.route("/start", methods=["POST"])
def start_script():
    try:
        # Change directory and execute the script
        os.chdir(script_dir)
        subprocess.Popen(["python3", script_name], shell=True)
        return "Script started successfully! Go back to <a href='/'>home</a>."
    except Exception as e:
        return f"Error: {e}. Go back to <a href='/'>home</a>."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
