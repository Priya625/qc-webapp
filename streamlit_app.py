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

# --- NEW: Load file_types for the dropdown (mirroring app.py) ---
try:
    file_types = list(config["file_types"].keys())
except KeyError:
    st.error("❌ FATAL ERROR: 'file_types' key not found in config.json. Please check your config.")
    st.stop()

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
#                STREAMLIT APP UI
# -----------------------------------------------------------
st.set_page_config(
    page_title=app_config.get("app_title", "BSR QC Tool"),
    page_icon=app_config.get("app_favicon", "✅"),
    layout="wide",
)

st.title(app_config.get("app_title", "BSR QC Tool"))
st.subheader(app_config.get("app_subtitle", "Automated Quality Checks"))

st.sidebar.header("File Upload")

# This replaces the <form> in Flask/HTML
uploaded_file = st.sidebar.file_uploader(
    "Upload BSR Excel file", type=["xlsx", "xls"]
)

# --- NEW: Add the file_type selectbox (mirroring app.py) ---
file_type = st.sidebar.selectbox("Select file type/market", options=file_types)

run_button = st.sidebar.button("Run QC")

st.sidebar.markdown("---")
st.sidebar.info(
    "Upload your BSR file, select the correct market/file type, and click 'Run QC'."
)

# -----------------------------------------------------------
#                QC LOGIC
# -----------------------------------------------------------

# This `if` block is the direct equivalent of the `@app.route("/run_qc")`
# function in your app.py.
if run_button and uploaded_file:
    try:
        # --- NEW: Find matching rules based on selected file_type ---
        file_rules = config["file_types"].get(file_type)
        if not file_rules:
            st.error(f"❌ No rules found for file type: {file_type}")
            st.stop() # This is the Streamlit equivalent of redirecting

        # --- NEW: Get settings from file_rules ---
        output_sheet = file_rules.get("output_sheet_name", "QC Report")
        input_sheet = file_rules.get("input_sheet_name", 0) # Use 0 (first sheet) as default
        
        # --- NEW: Create dynamic output file name ---
        output_file = f"QC_Report_{file_type}_{int(time.time())}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        logging.info(f"Reading uploaded file (sheet: {input_sheet})")
        
        # Read the file directly from memory, now using the correct input_sheet
        df = pd.read_excel(uploaded_file, sheet_name=input_sheet, engine="openpyxl")
        logging.info(f"DataFrame loaded. Shape: {df.shape}")

        # --- Run all QC checks ---
        st.info("Running QC checks... this may take a moment.")
        
        # --- UPDATED: Pass file_rules to the check function ---
        df = run_qc_checks(df, file_rules)

        # --- Handle datetime cleanup (no changes needed) ---
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

        # --- Save processed file ---
        logging.info(f"Saving processed file to {output_path}")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=output_sheet)

        # --- UPDATED: Pass file_rules to summary function ---
        color_excel(output_path, df)
        generate_summary_sheet(output_path, df, file_rules)

        st.success("✅ QC completed successfully!")
        logging.info(f"✅ QC completed successfully. Output saved → {output_path}")

        # --- Download Button ---
        with open(output_path, "rb") as f:
            st.download_button(
                label="⬇️ Download QC Result Excel",
                data=f.read(),
                file_name=output_file, # Use the new dynamic file name
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        logging.exception("Error during QC run")
        st.error(f"❌ An error occurred: {e}")