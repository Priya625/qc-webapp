import streamlit as st
import pandas as pd
import os
import json
import logging
import time
from qc_checks import *

# -----------------------------------------------------------
#                 CONFIGURATION SETUP
# -----------------------------------------------------------
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    st.error(f"❌ FATAL ERROR: config.json not found at {CONFIG_PATH}")
    st.stop()
except json.JSONDecodeError as e:
    st.error(f"❌ FATAL ERROR: config.json is invalid: {e}")
    st.stop()

app_config = config["app_settings"]

# -----------------------------------------------------------
#                 LOGGING SETUP
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
#                 FOLDER SETUP
# -----------------------------------------------------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------------------------------------
#                 FILE CLEANUP UTILITY
# -----------------------------------------------------------
def cleanup_old_files(folder_path, max_age_minutes=30):
    """Deletes files older than max_age_minutes in a given folder."""
    now = time.time()
    for filename in os.listdir(folder_path):
        path = os.path.join(folder_path, filename)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > (max_age_minutes * 60):
            try:
                os.remove(path)
                logging.info(f"🧹 Deleted old file: {path}")
            except Exception as e:
                logging.warning(f"⚠️ Error deleting {path}: {e}")

cleanup_old_files(UPLOAD_FOLDER, app_config.get("max_file_age_min", 30))
cleanup_old_files(OUTPUT_FOLDER, app_config.get("max_file_age_min", 30))

# -----------------------------------------------------------
#                 STREAMLIT UI
# -----------------------------------------------------------
st.set_page_config(page_title="QC Automation App", layout="wide")
st.title("⚙️ QC Automation WebApp")
st.caption("Upload your Rosco, BSR, and optional Macro file to run automated QC checks.")

# --- File Uploaders ---
rosco_file = st.file_uploader("📘 Upload Rosco File", type=["xlsx"])
bsr_file = st.file_uploader("📙 Upload BSR File", type=["xlsx"])
macro_file = st.file_uploader("📒 Upload Macro File (Optional)", type=["xlsx", "xls", "xlsm", "xlsb"])

# -----------------------------------------------------------
#                 RUN QC LOGIC
# -----------------------------------------------------------
if st.button("🚀 Run QC Checks"):
    if not rosco_file or not bsr_file:
        st.warning("⚠️ Please upload both Rosco and BSR files before running QC.")
        st.stop()

    with st.spinner("Running QC checks... This may take a few moments ⏳"):
        try:
            # --- Save uploaded files ---
            rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.name)
            bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.name)
            with open(rosco_path, "wb") as f:
                f.write(rosco_file.getbuffer())
            with open(bsr_path, "wb") as f:
                f.write(bsr_file.getbuffer())

            macro_path = None
            if macro_file:
                macro_path = os.path.join(UPLOAD_FOLDER, macro_file.name)
                with open(macro_path, "wb") as f:
                    f.write(macro_file.getbuffer())

            logging.info(f"📁 Uploaded → Rosco: {rosco_path}, BSR: {bsr_path}, Macro: {macro_path}")

            # --- Load Config Subsections ---
            col_map = config["column_mappings"]
            rules = config["qc_rules"]
            project = config["project_rules"]
            file_rules = config["file_rules"]

            # --- Detect Monitoring Period and Load BSR ---
            start_date, end_date = detect_period_from_rosco(rosco_path)
            df = load_bsr(bsr_path, col_map["bsr"])
            logging.info(f"📆 Monitoring period: {start_date} → {end_date}, Rows: {len(df)}")

            # --- Clean headers & values ---
            df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
            df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip() if isinstance(x, str) else x)
            df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

            # --- Run QC Checks (same order as Flask app) ---
            df = period_check(df, start_date, end_date, col_map["bsr"])
            df = completeness_check(df, col_map["bsr"], rules)
            df = overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"])
            df = program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
            df = check_event_matchday_competition(df, bsr_path, col_map, file_rules)
            df = market_channel_consistency_check(df, rosco_path, col_map, file_rules)
            df = domestic_market_check(df, project, col_map["bsr"], debug=True)
            df = rates_and_ratings_check(df, col_map["bsr"])
            df = duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)
            df = country_channel_id_check(df, col_map["bsr"])
            df = client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])
            df = rates_and_ratings_check(df, col_map["bsr"])  # repeated intentionally

            # --- Save Output ---
            output_prefix = file_rules.get("output_prefix", "QC_Result_")
            output_sheet = file_rules.get("output_sheet_name", "QC Results")
            output_file = f"{output_prefix}{os.path.splitext(bsr_file.name)[0]}.xlsx"
            output_path = os.path.join(OUTPUT_FOLDER, output_file)

            # --- Handle datetime cleanup ---
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

            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=output_sheet)

            color_excel(output_path, df)
            generate_summary_sheet(output_path, df, file_rules)

            st.success("✅ QC completed successfully!")
            logging.info(f"✅ QC completed successfully. Output saved → {output_path}")

            # --- Download Button ---
            with open(output_path, "rb") as f:
                st.download_button(
                    label="⬇️ Download QC Result Excel",
                    data=f.read(),
                    file_name=output_file,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        except Exception as e:
            logging.exception("❌ Error during QC run")
            st.error(f"❌ QC process failed: {e}")