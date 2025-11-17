from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import os
import time
import threading
import pandas as pd
import logging
import webbrowser
import json
from qc_checks import *

# -----------------------------------------------------------
#                CONFIGURATION SETUP
# -----------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    print(f" FATAL ERROR: config.json not found at {CONFIG_PATH}")
    exit(1)
except json.JSONDecodeError as e:
    print(f" FATAL ERROR: config.json is invalid: {e}")
    exit(1)

app_config = config["app_settings"]

# -----------------------------------------------------------
#                LOGGING SETUP
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(app_config.get("app_log_file", "app_debug.log")),
        logging.StreamHandler()
    ]
)

# -----------------------------------------------------------
#                FLASK APP SETUP
# -----------------------------------------------------------
app = Flask(__name__)
app.secret_key = app_config.get("secret_key", "default_secret_key")

BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------------------------------------
#                FILE CLEANUP UTILITY
# -----------------------------------------------------------
def cleanup_old_files(folder_path, max_age_minutes=30):
    now = time.time()
    for filename in os.listdir(folder_path):
        path = os.path.join(folder_path, filename)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > (max_age_minutes * 60):
            try:
                os.remove(path)
                logging.info(f" Deleted old file: {path}")
            except Exception as e:
                logging.warning(f" Error deleting {path}: {e}")

def start_background_cleanup():
    def loop_cleanup():
        while True:
            max_age = app_config.get("max_file_age_min", 30)
            cleanup_interval = app_config.get("cleanup_interval_sec", 300)
            cleanup_old_files(UPLOAD_FOLDER, max_age)
            cleanup_old_files(OUTPUT_FOLDER, max_age)
            time.sleep(cleanup_interval)

    threading.Thread(target=loop_cleanup, daemon=True).start()

start_background_cleanup()

# -----------------------------------------------------------
#                ROUTES
# -----------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/run_qc", methods=["POST"])
def run_qc():
    try:
        logging.info(" QC process started...")

        col_map = config["column_mappings"]
        rules = config["qc_rules"]
        project = config["project_rules"]
        file_rules = config["file_rules"]

        # Cleanup old files
        cleanup_old_files(UPLOAD_FOLDER, app_config.get("max_file_age_min", 30))
        cleanup_old_files(OUTPUT_FOLDER, app_config.get("max_file_age_min", 30))

        # Uploaded files
        rosco_file = request.files.get("rosco_file")
        bsr_file = request.files.get("bsr_file")
        data_file = request.files.get("data_file")
        macro_file = request.files.get("macro_file")

        if not rosco_file or not bsr_file:
            flash(" Please upload both Rosco and BSR files.")
            return redirect(url_for("index"))

        rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
        bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
        rosco_file.save(rosco_path)
        bsr_file.save(bsr_path)

        data_path = None
        if data_file:
            data_path = os.path.join(UPLOAD_FOLDER, data_file.filename)
            data_file.save(data_path)

        macro_path = None
        if macro_file:
            macro_path = os.path.join(UPLOAD_FOLDER, macro_file.filename)
            macro_file.save(macro_path)

        logging.info(f" Uploaded → Rosco: {rosco_path}, BSR: {bsr_path}, Data: {data_path}, Macro: {macro_path}")

        start_date, end_date = detect_period_from_rosco(rosco_path)

        df = load_bsr(bsr_path, col_map["bsr"])
        logging.info(f" Monitoring period: {start_date} → {end_date}, Rows: {len(df)}")

        # Clean headers & values
        df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
        df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip() if isinstance(x, str) else x)
        df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

        # -----------------------------------------------------------
        #   EXECUTION ORDER — IMPORTANT
        # -----------------------------------------------------------
        # 1️ Remaining QC checks
        df = period_check(df, start_date, end_date, col_map["bsr"])
        df = completeness_check(df, col_map["bsr"], rules)
        df = program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
        df = check_event_matchday_competition(df, bsr_path, col_map, file_rules)
        df = market_channel_consistency_check(df, rosco_path, col_map, file_rules)
        df = domestic_market_check(df, project, col_map["bsr"], debug=True)
        df = rates_and_ratings_check(df, col_map["bsr"])
        df = country_channel_id_check(df, col_map["bsr"])
        df = client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])
        df = rates_and_ratings_check(df, col_map["bsr"])

        # 2️ Duplicate Market Check FIRST — because Overlap depends on it
        df, duplicated_channels = duplicated_market_check(
            df, macro_path, project, col_map, file_rules, debug=True
        )

        # 3️ Overlap / Duplicate / Daybreak Check — pass duplicated channels
        df = overlap_duplicate_daybreak_check(
            df, col_map["bsr"], rules["overlap_check"], duplicated_channels=duplicated_channels
        )

        # -----------------------------------------------------------
        #   OUTPUT SAVE
        # -----------------------------------------------------------
        output_prefix = file_rules.get("output_prefix", "QC_Result_")
        output_sheet = file_rules.get("output_sheet_name", "QC Results")
        output_file = f"{output_prefix}{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        # Cleanup datetime formats
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=output_sheet)

        color_excel(output_path, df)
        generate_summary_sheet(output_path, df, file_rules)

        flash(" QC completed successfully!")
        logging.info(f" QC completed successfully. Output saved → {output_path}")

        return render_template("result.html", output_file=output_file)

    except Exception as e:
        logging.exception(" Error during QC run")
        flash(f" Error during QC: {str(e)}")
        return redirect(url_for("index"))

@app.route("/download/<path:output_file>")
def download(output_file):
    path = os.path.join(OUTPUT_FOLDER, output_file)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    flash(" File not found.")
    return redirect(url_for("index"))

# -----------------------------------------------------------
#                APP LAUNCH
# -----------------------------------------------------------
if __name__ == "__main__":
    port = app_config.get("port", 5000)
    host = app_config.get("host", "127.0.0.1")
    url = f"http://{host}:{port}/"

    logging.info(f" Flask app starting on {url}")

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        try:
            webbrowser.open_new(url)
        except Exception as e:
            logging.warning(f" Could not auto-open browser: {e}")

    app.run(debug=True, host=host, port=port)