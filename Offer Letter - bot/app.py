import json
import logging
import os
from collections import deque
from datetime import datetime
from threading import Event, Lock, Thread

from flask import Flask, jsonify, render_template, request

from offerletter_worker import (
    DEFAULT_FIELD_MAPPING,
    check_credentials_expiry,
    get_credentials_info,
    process_records,
    validate_credentials_payload,
)

# ================= Setup =================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
LOG_PATH = os.path.join(SCRIPT_DIR, "offerletter.log")

app = Flask(
    __name__,
    template_folder=os.path.join(SCRIPT_DIR, "templates"),
    static_folder=os.path.join(SCRIPT_DIR, "static"),
)

# ================= Logging =================

file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(file_handler)
root_logger.addHandler(logging.StreamHandler())

logger = logging.getLogger(__name__)

# ================= Scheduler State =================

scheduler_state = {
    "running": False,
    "last_run": None,
    "last_stats": None,
    "interval_minutes": 5,
    "scheduler_processing": False,
    "manual_processing": False,
    "active_run_type": None,
}

stop_event = Event()
scheduler_thread = None
processing_lock = Lock()


# ================= Config Management =================

def load_config():
    """Load config from config.json."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_config(config):
    """Save config to config.json."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def build_public_config(config):
    """Remove secrets before returning config to the browser."""
    public_config = dict(config)
    token = public_config.pop("airtable_api_token", None)
    if token:
        public_config["airtable_api_token_masked"] = token[:8] + "..." + token[-4:] if len(token) > 12 else "***"
    public_config["default_field_mapping"] = DEFAULT_FIELD_MAPPING
    public_config["field_mapping"] = {
        **DEFAULT_FIELD_MAPPING,
        **public_config.get("field_mapping", {}),
    }
    return public_config


# ================= Background Scheduler =================

def run_processing_job(run_type, config):
    """Run processing with a global lock so scheduler/manual runs cannot overlap."""
    if not processing_lock.acquire(blocking=False):
        logger.warning(f"{run_type} run skipped because another processing job is already active.")
        return None

    state_key = "scheduler_processing" if run_type == "Scheduled" else "manual_processing"
    scheduler_state[state_key] = True
    scheduler_state["active_run_type"] = run_type

    try:
        logger.info(f"=== {run_type} run starting ===")
        stats = process_records(config, SCRIPT_DIR)
        scheduler_state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        scheduler_state["last_stats"] = stats
        logger.info(f"=== {run_type} run complete ===")
        return stats
    except Exception as e:
        logger.error(f"{run_type} run error: {e}")
        return None
    finally:
        scheduler_state[state_key] = False
        scheduler_state["active_run_type"] = None
        processing_lock.release()


def scheduler_loop():
    """Background loop that runs processing at intervals."""
    logger.info(f"Scheduler started. Checking every {scheduler_state['interval_minutes']} minutes.")

    while not stop_event.is_set():
        config = load_config()
        if not config:
            logger.warning("No config found. Scheduler waiting...")
            stop_event.wait(60)
            continue

        if not check_credentials_expiry(SCRIPT_DIR):
            logger.warning("credentials.json missing. Scheduler pausing...")
            stop_event.wait(60)
            continue

        run_processing_job("Scheduled", config)
        stop_event.wait(scheduler_state["interval_minutes"] * 60)

    logger.info("Scheduler stopped.")


def start_scheduler():
    """Start the background scheduler thread."""
    global scheduler_thread
    if scheduler_state["running"]:
        return False

    stop_event.clear()
    scheduler_state["running"] = True
    scheduler_thread = Thread(target=scheduler_loop, daemon=True)
    scheduler_thread.start()
    return True


def stop_scheduler():
    """Stop the background scheduler thread."""
    global scheduler_thread
    if not scheduler_state["running"]:
        return False

    scheduler_state["running"] = False
    stop_event.set()
    if scheduler_thread:
        scheduler_thread.join(timeout=5)
        scheduler_thread = None
    return True


# ================= Routes =================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config", methods=["GET"])
def get_config():
    config = load_config()
    return jsonify(build_public_config(config))


@app.route("/api/config", methods=["POST"])
def post_config():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    config = load_config()

    for key in [
        "airtable_api_token",
        "base_id",
        "table_name",
        "view_name",
        "attachment_field_name",
        "template_doc_id",
        "target_folder_id",
    ]:
        if key not in data:
            continue
        value = data[key]
        if isinstance(value, str):
            value = value.strip()
        if not value:
            continue
        config[key] = value

    if "field_mapping" in data and isinstance(data["field_mapping"], dict):
        cleaned_mapping = {}
        for placeholder, field_name in data["field_mapping"].items():
            placeholder = str(placeholder).strip()
            field_name = str(field_name).strip()
            if placeholder and field_name:
                cleaned_mapping[placeholder] = field_name
        config["field_mapping"] = cleaned_mapping or dict(DEFAULT_FIELD_MAPPING)

    if "interval_minutes" in data:
        try:
            interval = int(data["interval_minutes"])
            if 1 <= interval <= 1440:
                config["interval_minutes"] = interval
                scheduler_state["interval_minutes"] = interval
        except (TypeError, ValueError):
            pass

    save_config(config)
    logger.info("Configuration saved.")
    return jsonify({"success": True})


@app.route("/api/upload-credentials", methods=["POST"])
def upload_credentials():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No file selected"}), 400

    try:
        content = file.read()
        payload = json.loads(content)
        validate_credentials_payload(payload)
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON file"}), 400
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    creds_path = os.path.join(SCRIPT_DIR, "credentials.json")
    with open(creds_path, "wb") as f:
        f.write(content)

    token_path = os.path.join(SCRIPT_DIR, "token.json")
    if os.path.exists(token_path):
        os.remove(token_path)
        logger.info("Old token.json deleted (new credentials uploaded).")

    logger.info("credentials.json uploaded successfully.")
    return jsonify({"success": True, "info": get_credentials_info(SCRIPT_DIR)})


@app.route("/api/status", methods=["GET"])
def get_status():
    return jsonify(
        {
            "scheduler_running": scheduler_state["running"],
            "scheduler_processing": scheduler_state["scheduler_processing"],
            "manual_processing": scheduler_state["manual_processing"],
            "processing_active": processing_lock.locked(),
            "active_run_type": scheduler_state["active_run_type"],
            "last_run": scheduler_state["last_run"],
            "last_stats": scheduler_state["last_stats"],
            "interval_minutes": scheduler_state["interval_minutes"],
            "credentials": get_credentials_info(SCRIPT_DIR),
        }
    )


@app.route("/api/logs", methods=["GET"])
def get_logs():
    try:
        lines = int(request.args.get("lines", 50))
    except (TypeError, ValueError):
        lines = 50
    lines = max(1, min(lines, 500))

    if not os.path.exists(LOG_PATH):
        return jsonify({"logs": []})

    with open(LOG_PATH, "r", encoding="utf-8") as f:
        recent = list(deque(f, maxlen=lines))

    return jsonify({"logs": [line.rstrip() for line in recent]})


def validate_required_config(config):
    required = [
        "airtable_api_token",
        "base_id",
        "table_name",
        "view_name",
        "template_doc_id",
        "target_folder_id",
    ]
    return [k for k in required if not config.get(k)]


@app.route("/api/start", methods=["POST"])
def start():
    config = load_config()
    missing = validate_required_config(config)
    if missing:
        return jsonify({"error": f"Missing config: {', '.join(missing)}"}), 400

    if not check_credentials_expiry(SCRIPT_DIR):
        return jsonify({"error": "credentials.json is missing. Please upload."}), 400

    if start_scheduler():
        return jsonify({"success": True, "message": "Scheduler started"})
    return jsonify({"error": "Scheduler is already running"}), 400


@app.route("/api/stop", methods=["POST"])
def stop():
    if stop_scheduler():
        return jsonify({"success": True, "message": "Scheduler stopped"})
    return jsonify({"error": "Scheduler is not running"}), 400


@app.route("/api/run-now", methods=["POST"])
def run_now():
    config = load_config()
    missing = validate_required_config(config)
    if missing:
        return jsonify({"error": f"Missing config: {', '.join(missing)}"}), 400

    if not check_credentials_expiry(SCRIPT_DIR):
        return jsonify({"error": "credentials.json is missing."}), 400

    if processing_lock.locked():
        run_type = scheduler_state["active_run_type"] or "Another"
        return jsonify({"error": f"{run_type} run is already in progress."}), 400

    thread = Thread(target=lambda: run_processing_job("Manual", config), daemon=True)
    thread.start()
    return jsonify({"success": True, "message": "Manual run started"})


# ================= Main =================

if __name__ == "__main__":
    config = load_config()
    if config.get("interval_minutes"):
        scheduler_state["interval_minutes"] = config["interval_minutes"]

    logger.info("Offer Letter Bot starting on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
