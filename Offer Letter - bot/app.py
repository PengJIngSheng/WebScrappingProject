import os
import json
import logging
from datetime import datetime
from threading import Thread, Event
from flask import Flask, render_template, request, jsonify
from offerletter_worker import (
    process_records,
    get_credentials_info,
    check_credentials_expiry,
    DEFAULT_FIELD_MAPPING
)

# ================= Setup =================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, 'config.json')
LOG_PATH = os.path.join(SCRIPT_DIR, 'offerletter.log')

app = Flask(__name__)

# ================= Logging =================

# File handler for persistent logs
file_handler = logging.FileHandler(LOG_PATH, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

# Configure root logger
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
    "manual_processing": False
}

stop_event = Event()
scheduler_thread = None


# ================= Config Management =================

def load_config():
    """Load config from config.json."""
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_config(config):
    """Save config to config.json."""
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


# ================= Background Scheduler =================

def scheduler_loop():
    """Background loop that runs processing at intervals."""
    logger.info(f"Scheduler started. Checking every {scheduler_state['interval_minutes']} minutes.")

    while not stop_event.is_set():
        config = load_config()
        if not config:
            logger.warning("No config found. Scheduler waiting...")
            stop_event.wait(60)
            continue

        # Check credentials expiry
        if not check_credentials_expiry(SCRIPT_DIR):
            logger.warning("credentials.json expired or missing. Scheduler pausing...")
            stop_event.wait(60)
            continue

        try:
            scheduler_state["scheduler_processing"] = True
            logger.info("=== Scheduled run starting ===")
            stats = process_records(config, SCRIPT_DIR)
            scheduler_state["last_run"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            scheduler_state["last_stats"] = stats
            logger.info(f"=== Scheduled run complete ===")
        except Exception as e:
            logger.error(f"Scheduler run error: {e}")
        finally:
            scheduler_state["scheduler_processing"] = False

        # Wait for interval or until stopped
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

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/config', methods=['GET'])
def get_config():
    config = load_config()
    # Never return token in full for security — mask it
    if config.get("airtable_api_token"):
        token = config["airtable_api_token"]
        config["airtable_api_token_masked"] = token[:8] + "..." + token[-4:] if len(token) > 12 else "***"
    config["default_field_mapping"] = DEFAULT_FIELD_MAPPING
    return jsonify(config)


@app.route('/api/config', methods=['POST'])
def post_config():
    data = request.json
    if not data:
        return jsonify({"error": "No data provided"}), 400

    config = load_config()

    # Update fields
    for key in ["airtable_api_token", "base_id", "table_name", "view_name",
                "attachment_field_name", "template_doc_id", "target_folder_id"]:
        if key in data and data[key]:
            config[key] = data[key].strip()

    # Field mapping
    if "field_mapping" in data and isinstance(data["field_mapping"], dict):
        config["field_mapping"] = data["field_mapping"]

    # Interval
    if "interval_minutes" in data:
        try:
            interval = int(data["interval_minutes"])
            if 1 <= interval <= 1440:
                config["interval_minutes"] = interval
                scheduler_state["interval_minutes"] = interval
        except (ValueError, TypeError):
            pass

    save_config(config)
    logger.info("Configuration saved.")
    return jsonify({"success": True})


@app.route('/api/upload-credentials', methods=['POST'])
def upload_credentials():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    # Validate it's JSON
    try:
        content = file.read()
        json.loads(content)
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON file"}), 400

    # Save credentials.json
    creds_path = os.path.join(SCRIPT_DIR, 'credentials.json')
    with open(creds_path, 'wb') as f:
        f.write(content)

    # Delete old token.json to force re-auth
    token_path = os.path.join(SCRIPT_DIR, 'token.json')
    if os.path.exists(token_path):
        os.remove(token_path)
        logger.info("Old token.json deleted (new credentials uploaded).")

    logger.info("credentials.json uploaded successfully.")
    return jsonify({"success": True, "info": get_credentials_info(SCRIPT_DIR)})


@app.route('/api/status', methods=['GET'])
def get_status():
    return jsonify({
        "scheduler_running": scheduler_state["running"],
        "scheduler_processing": scheduler_state["scheduler_processing"],
        "manual_processing": scheduler_state["manual_processing"],
        "last_run": scheduler_state["last_run"],
        "last_stats": scheduler_state["last_stats"],
        "interval_minutes": scheduler_state["interval_minutes"],
        "credentials": get_credentials_info(SCRIPT_DIR)
    })


@app.route('/api/logs', methods=['GET'])
def get_logs():
    lines = int(request.args.get('lines', 50))
    if not os.path.exists(LOG_PATH):
        return jsonify({"logs": []})

    with open(LOG_PATH, 'r', encoding='utf-8') as f:
        all_lines = f.readlines()

    recent = all_lines[-lines:] if len(all_lines) > lines else all_lines
    return jsonify({"logs": [line.rstrip() for line in recent]})


@app.route('/api/start', methods=['POST'])
def start():
    config = load_config()
    required = ["airtable_api_token", "base_id", "table_name", "view_name",
                "template_doc_id", "target_folder_id"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        return jsonify({"error": f"Missing config: {', '.join(missing)}"}), 400

    if not check_credentials_expiry(SCRIPT_DIR):
        return jsonify({"error": "credentials.json is missing or expired. Please upload."}), 400

    if start_scheduler():
        return jsonify({"success": True, "message": "Scheduler started"})
    return jsonify({"error": "Scheduler is already running"}), 400


@app.route('/api/stop', methods=['POST'])
def stop():
    if stop_scheduler():
        return jsonify({"success": True, "message": "Scheduler stopped"})
    return jsonify({"error": "Scheduler is not running"}), 400


@app.route('/api/run-now', methods=['POST'])
def run_now():
    config = load_config()
    required = ["airtable_api_token", "base_id", "table_name", "view_name",
                "template_doc_id", "target_folder_id"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        return jsonify({"error": f"Missing config: {', '.join(missing)}"}), 400

    if not check_credentials_expiry(SCRIPT_DIR):
        return jsonify({"error": "credentials.json is missing or expired."}), 400

    if scheduler_state["manual_processing"]:
        return jsonify({"error": "A manual run is already in progress."}), 400

    def run_in_background():
        scheduler_state["manual_processing"] = True
        try:
            logger.info("=== Manual run starting ===")
            stats = process_records(config, SCRIPT_DIR)
            scheduler_state["last_run"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            scheduler_state["last_stats"] = stats
            logger.info("=== Manual run complete ===")
        except Exception as e:
            logger.error(f"Manual run error: {e}")
        finally:
            scheduler_state["manual_processing"] = False

    thread = Thread(target=run_in_background, daemon=True)
    thread.start()
    return jsonify({"success": True, "message": "Manual run started"})


# ================= Main =================

if __name__ == '__main__':
    # Load interval from config if saved
    config = load_config()
    if config.get("interval_minutes"):
        scheduler_state["interval_minutes"] = config["interval_minutes"]

    logger.info("Offer Letter Bot starting on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)
