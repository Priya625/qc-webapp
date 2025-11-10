import streamlit as st
import pandas as pd
import os
import json
import logging
import time
from qc_checks import *

# --- Load Configuration ---
try:
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
except FileNotFoundError:
    st.error("❌ FATAL ERROR: config.json not found. The application cannot start.")
    st.stop()
except json.JSONDecodeError as e:
    st.error(f"❌ FATAL ERROR: config.json is not valid JSON: {e}")
    st.stop()

# --- Read config sections ---
app_config = config["app_settings"]
col_map = config["column_mappings"]
rules = config["qc_rules"]
project = config["project_rules"]
file_rules = config["file_rules"]

# --- Setup folders ---
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --- Cleanup old files ---
def cleanup_old_files(folder_path, max_age_minutes=30):
    now = time.time()
    for filename in os.listdir(folder_path):
        path = os.path.join(folder_path, filename)
        if os.path.isfile(path) and (now - os.path.getmtime(path)) > (max_age_minutes * 60):
            try:
                os.remove(path)
            except Exception:
                pass

cleanup_old_files(UPLOAD_FOLDER, app_config.get("max_file_age_min", 30))
cleanup_old_files(OUTPUT_FOLDER, app_config.get("max_file_age_min", 30))

# --- Streamlit UI ---
st.set_page_config(page_title="QC Automation App", layout="wide")
st.title("⚙️ QC Automation WebApp")
st.caption("Upload your Rosco, BSR, and Macro files to run automated quality checks.")

# --- File Uploaders ---
rosco_file = st.file_uploader("📘 Upload Rosco File", type=["xlsx"])
bsr_file = st.file_uploader("📙 Upload BSR File", type=["xlsx"])
macro_file = st.file_uploader("📒 Upload Macro File (Optional)", type=["xlsx"])

if st.button("🚀 Run QC Checks"):
    if not rosco_file or not bsr_file:
        st.warning("⚠️ Please upload both Rosco and BSR files before running QC.")
        st.stop()

    with st.spinner("Running QC checks... This may take a few moments ⏳"):
        try:
            # --- Save uploaded files locally ---
            rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.name)
            bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.name)
            macro_path = os.path.join(UPLOAD_FOLDER, macro_file.name) if macro_file else None

            with open(rosco_path, "wb") as f: f.write(rosco_file.getbuffer())
            with open(bsr_path, "wb") as f: f.write(bsr_file.getbuffer())
            if macro_file:
                with open(macro_path, "wb") as f: f.write(macro_file.getbuffer())

            # --- Detect monitoring period ---
            start_date, end_date = detect_period_from_rosco(rosco_path)
            df = load_bsr(bsr_path, col_map["bsr"])

            # --- Run QC Checks (same as app.py) ---
            df = period_check(df, start_date, end_date, col_map["bsr"])
            df = completeness_check(df, col_map["bsr"], rules["program_category"])
            df = overlap_duplicate_daybreak_check(df, col_map["bsr"], rules["overlap_check"])
            df = program_category_check(bsr_path, df, col_map, rules["program_category"], file_rules)
            df = check_event_matchday_competition(df, bsr_path, col_map, file_rules)
            df = market_channel_consistency_check(df, rosco_path, col_map, file_rules)
            df = domestic_market_check(df, project, col_map["bsr"], debug=True)
            df = rates_and_ratings_check(df, col_map["bsr"])
            df = duplicated_market_check(df, macro_path, project, col_map, file_rules, debug=True)
            df = country_channel_id_check(df, col_map["bsr"])
            df = client_lstv_ott_check(df, col_map["bsr"], rules["client_check"])
            df = rates_and_ratings_check(df, col_map["bsr"])

            # --- Save Excel Output ---
            output_prefix = file_rules.get("output_prefix", "QC_Result_")
            output_sheet = file_rules.get("output_sheet_name", "QC Results")
            output_file = f"{output_prefix}{os.path.splitext(bsr_file.name)[0]}.xlsx"
            output_path = os.path.join(OUTPUT_FOLDER, output_file)

            # Clean datetime columns before saving
            for col in df.select_dtypes(include=["datetimetz"]).columns:
                df[col] = df[col].dt.tz_localize(None)

            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name=output_sheet)

            # --- Apply color formatting & summary ---
            color_excel(output_path, df)
            generate_summary_sheet(output_path, df, file_rules)

            # --- Display completion ---
            st.success("✅ QC completed successfully!")
            st.download_button(
                label="⬇️ Download QC Result Excel",
                data=open(output_path, "rb").read(),
                file_name=output_file,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"❌ QC process failed: {e}")
            st.stop()