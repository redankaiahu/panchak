import subprocess
import sys
import os
from datetime import datetime

def run_command(command):
    """Runs a shell command and returns output."""
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ SUCCESS: {command}")
            if result.stdout: print(result.stdout.strip())
            return True
        else:
            print(f"❌ ERROR: {command}")
            if result.stderr: print(result.stderr.strip())
            return False
    except Exception as e:
        print(f"❗ Exception running {command}: {e}")
        return False

def sync_pull():
    print("\n⬇️  PULLING LATEST STATE FROM GITHUB...")
    # Stash local changes to avoid conflicts during pull
    run_command("git stash")
    success = run_command("git pull --rebase origin main")
    # Bring back local changes
    run_command("git stash pop")
    if success:
        print("✅ Sync Pull Complete.")
    else:
        print("⚠️  Sync Pull might have failed or found nothing to update.")

def sync_push():
    print(f"\n⬆️  PUSHING TRADING STATE TO GITHUB...")
    
    # 1. Add specific patterns only
    patterns = [
        "execution_log*", "paper_trades*", "positions_cache*",
        "multi_execution*", "multi_paper*", "multi_positions*",
        "heatmap_nifty*", "Telegram_Trade*"
    ]
    run_command(f"git add {' '.join(patterns)}")
    
    # Also add the core scripts if they changed
    run_command("git add *.py .gitignore")
    
    # 2. Check if there are changes to commit
    result = subprocess.run("git status --porcelain", shell=True, capture_output=True, text=True)
    if not result.stdout.strip():
        print("ℹ️  No new state changes to push.")
        return

    # 3. Commit with timestamp
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_command(f'git commit -m "Auto-sync state: {ts}"')
    
    # 4. Pull first to ensure no conflicts, then push
    run_command("git pull --rebase origin main")
    if run_command("git push origin main"):
        print("✅ Sync Push Complete.")
    else:
        print("❌ Sync Push Failed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python sync_state.py [pull|push]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    if action == "pull":
        sync_pull()
    elif action == "push":
        sync_push()
    else:
        print(f"Unknown action: {action}")
