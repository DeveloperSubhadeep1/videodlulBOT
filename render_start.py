
import os
import sys
import base64
import subprocess
import signal
from pathlib import Path

from flask import Flask, jsonify
from waitress import serve

BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)

app = Flask(__name__)
bot_process = None


@app.route("/")
def home():
    return "Multi-Platform Video Downloader is running!"


@app.route("/health")
def health():
    running = bot_process is not None and bot_process.poll() is None

    return jsonify({
        "status": "online",
        "telegram_bot": "running" if running else "stopped"
    }), 200 if running else 503


def start_bot():
    global bot_process

    session_path = BASE_DIR / "uploader.session"

    if not session_path.exists():
        raise RuntimeError("uploader.session file is missing")

        

    else:
        session_path = BASE_DIR / "uploader.session"
        if not session_path.exists():
            raise RuntimeError("Telegram uploader session is missing")

    bot_process = subprocess.Popen(
        [sys.executable, "-u", "bot.py"],
        cwd=str(BASE_DIR)
    )

    print(f"Telegram bot started: PID {bot_process.pid}", flush=True)


def shutdown(signum, frame):
    if bot_process and bot_process.poll() is None:
        bot_process.terminate()
        try:
            bot_process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            bot_process.kill()

    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    start_bot()

    port = int(os.environ.get("PORT", 10000))

    print(f"Flask server listening on port {port}", flush=True)

    serve(app, host="0.0.0.0", port=port)
