import streamlit as st
import pandas as pd
import os
import json
import logging
import time
from qc_checks import * # This import brings in all the core logic (e.g., run_qc_checks)

# -----------------------------------------------------------\r
#                 CONFIGURATION SETUP
# -----------------------------------------------------------\r
# This logic is identical to app.py
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
file_types = list(config["file_types"].keys())

# -----------------------------------------------------------\r
#                 LOGGING SETUP
# -----------------------------------------------------------\r
# This logic is identical to app.py
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(app_config.get("app_log_file", "app_debug.log")),
        logging.StreamHandler(),
    ],
)

# -----------------------------------------------------------\r
#                 FOLDER SETUP
# -----------------------------------------------------------\r
# This logic is identical to app.py
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# -----------------------------------------------------------\r
#                STREAMLIT APP UI
# -----------------------------------------------------------\r
# This section replaces all the Flask `render_template` and HTML files.
# In Flask (app.py), this is handled by `@app.route("/")` and `index.html`.
st.set_page_config(
    page_title=app_config.get("app_title", "BSR QC Tool"),
    page_icon=app_config.get("app_favicon", "✅"),
    layout="wide",
)

st.title(app_config.get("app_title", "BSR QC Tool"))
st.subheader(app_config.get("app_subtitle", "Automated Quality Checks"))

st.sidebar.header("File Upload")

# This replaces the <form> in Flask/HTML.
# `uploaded_file` is the Streamlit equivalent of Flask's `request.files["bsr_file"]`
uploaded_file = st.sidebar.file_uploader(
    "Upload BSR Excel file", type=["xlsx", "xls"]
)

# This is the equivalent of Flask's `request.form["file_type"]`
file_type = st.sidebar.selectbox("Select file type/market", options=file_types)

# This is the equivalent of the <button type="submit"> in a Flask form.
run_button = st.sidebar.button("Run QC")

st.sidebar.markdown("---")
st.sidebar.info(
    "Upload your BSR file, select the correct market/file type, and click 'Run QC'."
)

# -----------------------------------------------------------\r
#                QC LOGIC
# -----------------------------------------------------------\r

# This `if` block is the direct equivalent of the `@app.route("/run_qc")`
# function in your app.py. It runs when the user clicks the button.
if run_button and uploaded_file:
    try:
        # --- Find matching rules ---
        # This logic is identical to app.py lines 96-100
        file_rules = config["file_types"].get(file_type)
        if not file_rules:
            # This `st.error` is the Streamlit equivalent of Flask's `flash(...)`
            st.error(f"❌ No rules found for file type: {file_type}")
            st.stop()

        # --- Setup paths and read data ---
        # This logic is identical to app.py lines 102-108
        output_sheet = file_rules.get("output_sheet_name", "QC Report")
        input_sheet = file_rules.get("input_sheet_name", 0)
        output_file = f"QC_Report_{file_type}_{int(time.time())}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        logging.info(f"Reading uploaded file (sheet: {input_sheet})")
        
        # This is slightly different from app.py, and *better*.
        # app.py saves the file to disk and then reads it.
        # Streamlit reads the file directly from the in-memory upload.
        df = pd.read_excel(uploaded_file, sheet_name=input_sheet, engine="openpyxl")
        logging.info(f"DataFrame loaded. Shape: {df.shape}")

        # --- Run all QC checks ---
        st.info("Running QC checks... this may take a moment.")
        # This is the main call, identical to app.py line 112
        df = run_qc_checks(df, file_rules)

        # --- Handle datetime cleanup ---
        # This cleanup logic is identical to app.py lines 114-121
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
        # This logic is identical to app.py lines 123-126
        logging.info(f"Saving processed file to {output_path}")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=output_sheet)

        # These calls are identical to app.py lines 128-129
        color_excel(output_path, df)
        generate_summary_sheet(output_path, df, file_rules)

        # `st.success` is the equivalent of Flask's `flash("QC completed...")`
        # and `render_template("result.html")`
        st.success("✅ QC completed successfully!")
        logging.info(f"✅ QC completed successfully. Output saved → {output_path}")

        # --- Download Button ---
        # This `st.download_button` replaces the entire `@app.route("/download/...")`
        # function from app.py (lines 134-140)
        with open(output_path, "rb") as f:
            st.download_button(
                label="⬇️ Download QC Result Excel",
                data=f.read(),
                file_name=output_file,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        # This `st.error` is the Streamlit equivalent of Flask's error flashing
        logging.exception("Error during QC run")
        st.error(f"❌ An error occurred: {e}")