# streamlit_app.py
import streamlit as st
import os
import time
import threading
import pandas as pd
import logging
import json
from io import BytesIO
from qc_checks import *  # keep your existing QC functions: load_bsr, detect_period_from_rosco, etc.

# -----------------------------------------------------------
#                CONFIGURATION SETUP
# -----------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    st.error(f"FATAL ERROR: config.json not found at {CONFIG_PATH}")
    st.stop()
except json.JSONDecodeError as e:
    st.error(f"FATAL ERROR: config.json is invalid: {e}")
    st.stop()

# Validate required config sections early and fail gracefully with a helpful message
required_keys = ["column_mappings", "qc_rules", "project_rules", "file_rules", "app_settings"]
missing = [k for k in required_keys if k not in config]
if missing:
    st.error(
        "FATAL ERROR: config.json is missing required keys: "
        f"{', '.join(missing)}. Please ensure the config file includes these sections."
    )
    # show what keys are present to aid debugging
    st.write("Keys present in loaded config:", list(config.keys()))
    st.stop()

app_config = config.get("app_settings", {})

# -----------------------------------------------------------
#                LOGGING SETUP
# -----------------------------------------------------------
log_file = app_config.get("app_log_file", "app_debug.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, log_file)),
        logging.StreamHandler()
    ]
)

# -----------------------------------------------------------
#                FILE PATHS
# -----------------------------------------------------------
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -----------------------------------------------------------
#                FILE CLEANUP UTILITY (background)
# -----------------------------------------------------------
def cleanup_old_files(folder_path, max_age_minutes=30):
    now = time.time()
    for filename in os.listdir(folder_path):
        path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(path) and (now - os.path.getmtime(path)) > (max_age_minutes * 60):
                os.remove(path)
                logging.info(f"Deleted old file: {path}")
        except Exception as e:
            logging.warning(f"Error deleting {path}: {e}")

def start_background_cleanup():
    def loop_cleanup():
        while True:
            max_age = app_config.get("max_file_age_min", 30)
            cleanup_interval = app_config.get("cleanup_interval_sec", 300)
            cleanup_old_files(UPLOAD_FOLDER, max_age)
            cleanup_old_files(OUTPUT_FOLDER, max_age)
            time.sleep(cleanup_interval)

    t = threading.Thread(target=loop_cleanup, daemon=True)
    t.start()

# Start cleanup once (safe to call multiple times)
if "cleanup_started" not in st.session_state:
    start_background_cleanup()
    st.session_state["cleanup_started"] = True

# -----------------------------------------------------------
#                Helper utilities
# -----------------------------------------------------------
def save_uploaded_file(uploaded_file, dest_folder):
    """
    Save a streamlit UploadedFile to destination folder and return the saved path.
    """
    filename = uploaded_file.name
    path = os.path.join(dest_folder, filename)
    # ensure unique name if needed
    base, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(path):
        path = os.path.join(dest_folder, f"{base}_{counter}{ext}")
        counter += 1
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path

def cleanup_datetime_columns(df):
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        try:
            df[col] = df[col].dt.tz_localize(None)
        except Exception:
            # ignore if cannot localize
            pass
    return df

# -----------------------------------------------------------
#                Streamlit UI
# -----------------------------------------------------------
st.set_page_config(page_title="QC Runner", layout="wide")
st.title("QC Runner (Streamlit)")

st.header("Upload files")
st.write("Upload the required files (Rosco and BSR). Optional files: Data, Macro.")

rosco_file = st.file_uploader("Rosco file (required)", type=None, key="rosco")
bsr_file = st.file_uploader("BSR file (required)", type=None, key="bsr")
macro_file = st.file_uploader("Macro file (optional)", type=None, key="macro")

run_button = st.button("Run QC")

# Show logs area
log_container = st.empty()

# Main run
if run_button:
    if rosco_file is None or bsr_file is None:
        st.error("Please upload both Rosco and BSR files.")
    else:
        try:
            with st.spinner("Running QC..."):
                logging.info("QC process started (Streamlit)")

                # Safely fetch required config sections (we validated earlier)
                col_map = config.get("column_mappings")
                rules = config.get("qc_rules")
                project = config.get("project_rules")
                file_rules = config.get("file_rules")

                # Sanity check (shouldn't hit due to earlier validation)
                if not col_map or not rules or not project or not file_rules:
                    st.error("Configuration sections missing after initial validation. Aborting.")
                    logging.error("Configuration sections missing during run.")
                    st.stop()

                # Cleanup old files first (immediate)
                cleanup_old_files(UPLOAD_FOLDER, app_config.get("max_file_age_min", 30))
                cleanup_old_files(OUTPUT_FOLDER, app_config.get("max_file_age_min", 30))

                # Save uploads to disk
                rosco_path = save_uploaded_file(rosco_file, UPLOAD_FOLDER)
                bsr_path = save_uploaded_file(bsr_file, UPLOAD_FOLDER)
                logging.info(f"Uploaded → Rosco: {rosco_path}, BSR: {bsr_path}")

                data_path = None
                if data_file:
                    data_path = save_uploaded_file(data_file, UPLOAD_FOLDER)
                    logging.info(f"Uploaded Data: {data_path}")

                macro_path = None
                if macro_file:
                    macro_path = save_uploaded_file(macro_file, UPLOAD_FOLDER)
                    logging.info(f"Uploaded Macro: {macro_path}")

                # Detect period from rosco (re-using your function)
                start_date, end_date = detect_period_from_rosco(rosco_path)
                logging.info(f"Monitoring period: {start_date} → {end_date}")

                # Load BSR using your existing loader
                df = load_bsr(bsr_path, col_map["bsr"])
                logging.info(f"Rows loaded: {len(df)}")

                # Clean headers & values (same normalization you had)
                df.columns = df.columns.str.strip().str.replace("\xa0", " ", regex=True)
                df = df.applymap(lambda x: str(x).replace("\xa0", " ").strip() if isinstance(x, str) else x)
                df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

                # -----------------------------------------------------------
                #   EXECUTION ORDER — IMPORTANT (same steps as your Flask app)
                # -----------------------------------------------------------
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

                # Duplicate Market Check FIRST
                df, duplicated_channels = duplicated_market_check(
                    df, macro_path, project, col_map, file_rules, debug=True
                )

                # Overlap / Duplicate / Daybreak Check
                df = overlap_duplicate_daybreak_check(
                    df, col_map["bsr"], rules["overlap_check"], duplicated_channels=duplicated_channels
                )

                # -----------------------------------------------------------
                #   OUTPUT SAVE
                # -----------------------------------------------------------
                output_prefix = file_rules.get("output_prefix", "QC_Result_")
                output_sheet = file_rules.get("output_sheet_name", "QC Results")
                output_file = f"{output_prefix}{os.path.splitext(bsr_file.name)[0]}.xlsx"
                output_path = os.path.join(OUTPUT_FOLDER, output_file)

                # Cleanup datetime formats
                df = cleanup_datetime_columns(df)

                with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name=output_sheet)

                # apply your formatting and summary (these functions are from qc_checks)
                try:
                    color_excel(output_path, df)
                except Exception as e:
                    logging.warning(f"color_excel failed: {e}")

                try:
                    generate_summary_sheet(output_path, df, file_rules)
                except Exception as e:
                    logging.warning(f"generate_summary_sheet failed: {e}")

                logging.info(f"QC completed successfully. Output saved → {output_path}")
                st.success("QC completed successfully!")

                # Display download button and a preview of top rows
                st.write("### Output")
                st.write(f"Saved: `{output_path}`")
                with open(output_path, "rb") as f:
                    data = f.read()
                    st.download_button(
                        label="Download QC Result (.xlsx)",
                        data=data,
                        file_name=output_file,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                # Show a small preview of the results dataframe
                st.write("### Results preview (first 10 rows)")
                st.dataframe(df.head(10))

        except Exception as e:
            logging.exception("Error during QC run")
            st.error(f"Error during QC: {e}")
            # optionally show stack trace
            import traceback
            st.text(traceback.format_exc())