import subprocess
import time

ENGINE_FILE = "kite_execution_engine.py"

while True:
    print("🚀 Starting Execution Engine...")
    process = subprocess.Popen(["python", ENGINE_FILE])

    process.wait()

    print("⚠ Engine Crashed. Restarting in 5 seconds...")
    time.sleep(5)