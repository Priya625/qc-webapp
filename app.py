from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import os
import time
import threading
import pandas as pd
import logging
import webbrowser
from qc_checks import *

# ---------------- Logging Setup ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app_debug.log"),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)
app.secret_key = "qc_secret_key"

BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ---------------- Cleanup old files ----------------
def cleanup_old_files(folder_path, max_age_minutes=30):
    now = time.time()
    for filename in os.listdir(folder_path):
        path = os.path.join(folder_path, filename)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > (max_age_minutes * 60):
            try:
                os.remove(path)
                logging.info(f"🧹 Deleted old file: {path}")
            except Exception as e:
                logging.warning(f"⚠️ Error deleting {path}: {e}")

def start_background_cleanup():
    def loop_cleanup():
        while True:
            cleanup_old_files(UPLOAD_FOLDER)
            cleanup_old_files(OUTPUT_FOLDER)
            time.sleep(300)
    threading.Thread(target=loop_cleanup, daemon=True).start()

start_background_cleanup()

# ---------------- Routes ----------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/run_qc", methods=["POST"])
def run_qc():
    try:
        logging.info("🚀 QC process started...")

        cleanup_old_files(UPLOAD_FOLDER)
        cleanup_old_files(OUTPUT_FOLDER)

        # --- Uploaded files ---
        rosco_file = request.files.get("rosco_file")
        bsr_file = request.files.get("bsr_file")
        data_file = request.files.get("data_file")  # Optional
        macro_file = request.files.get("macro_file")  # New macro upload

        if not rosco_file or not bsr_file:
            flash("⚠️ Please upload both Rosco and BSR files.")
            return redirect(url_for("index"))

        # --- Save uploaded files ---
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

        logging.info(f"📁 Uploaded Files → Rosco: {rosco_path}, BSR: {bsr_path}, Data: {data_path}, Macro: {macro_path}")

        # === Detect monitoring period and load data ===
        start_date, end_date = detect_period_from_rosco(rosco_path)
        df = load_bsr(bsr_path)
        logging.info(f"📆 Monitoring period: {start_date} → {end_date}, Rows loaded: {len(df)}")

        # === Clean headers and text values ===
        df.columns = df.columns.str.strip().str.replace('\xa0', ' ', regex=True)
        df = df.applymap(lambda x: str(x).replace('\xa0', ' ').strip() if isinstance(x, str) else x)

        rename_map = {
            "Start(UTC)": "Start (UTC)",
            "End(UTC)": "End (UTC)",
        }
        df.rename(columns=rename_map, inplace=True)

        # === Run QC Checks ===
        df = period_check(df, start_date, end_date)
        df = completeness_check(df)
        df = overlap_duplicate_daybreak_check(df)
        df = program_category_check(bsr_path, df)
        df = duration_check(df)
        df = check_event_matchday_competition(df, bsr_path)
        df = market_channel_consistency_check(df, rosco_path, bsr_path)

        # 🟡 Domestic Market Coverage Check (LaLiga logic)
        df = domestic_market_check(df, league_keyword="F24 Spain", debug=True)

        # 🟢 Duplicated Markets Check (using macro file)
        df = duplicated_market_check(df, macro_path, league_keyword="F24 Spain", debug=True)

        df = country_channel_id_check(df)
        df = client_lstv_ott_check(df)
        df = rates_and_ratings_check(df)

        # === Save Output ===
        output_file = f"QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        # Handle datetime cleanup
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_localize(None)

        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    df[col] = pd.to_datetime(df[col], errors="ignore")
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = df[col].dt.tz_localize(None)
                except Exception:
                    pass

        # Save and format Excel
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="QC Results")

        color_excel(output_path, df)
        generate_summary_sheet(output_path, df)

        flash("✅ QC completed successfully!")
        logging.info(f"✅ QC completed successfully. Output saved to {output_path}")
        return render_template("result.html", output_file=output_file)

    except Exception as e:
        logging.exception("❌ Error during QC run")
        flash(f"❌ Error during QC: {str(e)}")
        return redirect(url_for("index"))

@app.route("/download/<path:output_file>")
def download(output_file):
    path = os.path.join(OUTPUT_FOLDER, output_file)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    flash("⚠️ File not found.")
    return redirect(url_for("index"))

# ---------------- App Launch ----------------
if __name__ == "__main__":
    port = 5000
    url = f"http://127.0.0.1:{port}/"
    logging.info(f"🌐 Flask app starting on {url}")

    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        try:
            webbrowser.open_new(url)
        except Exception as e:
            logging.warning(f"⚠️ Could not auto-open browser: {e}")

    app.run(debug=True, host="127.0.0.1", port=port)