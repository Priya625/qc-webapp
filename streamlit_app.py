import streamlit as st
import os
import time
import threading
import pandas as pd
import logging
from io import BytesIO

# ✅ Import all QC check functions
from qc_checks import (
    detect_period_from_rosco,
    load_bsr,
    period_check,
    completeness_check,
    overlap_duplicate_daybreak_check,
    program_category_check,
    duration_check,
    check_event_matchday_competition,
    market_channel_consistency_check,
    domestic_market_check,
    rates_and_ratings_check,
    duplicated_market_check,
    country_channel_id_check,
    client_lstv_ott_check,
    color_excel,
    generate_summary_sheet,
)

# ---------------- Logging Setup ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("streamlit_debug.log"), logging.StreamHandler()],
)

# ---------------- Folder Setup ----------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ---------------- Cleanup Old Files ----------------
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
    t = threading.Thread(target=loop_cleanup, daemon=True)
    t.start()

start_background_cleanup()

# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="QC Automation App", layout="wide")
st.title("🧾 Automated QC Checker")

st.markdown("""
Upload your **Rosco** and **BSR** files below to run the QC checks.  
You can optionally upload the **Macro Market Duplicator (.xlsm)** file for the duplicated markets check.  
A downloadable Excel QC report will be generated.
""")

# ---------------- Upload Section ----------------
rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="rosco")
bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx", "xls", "xlsm"], key="bsr")
macro_file = st.file_uploader("📙 Optional: Upload Macro Market Duplicator (.xlsm)", type=["xlsm", "xlsx"], key="macro")

# ---------------- Run QC Button ----------------
if st.button("🚀 Run QC Checks"):
    if not rosco_file or not bsr_file:
        st.error("⚠️ Please upload both Rosco and BSR files.")
    else:
        try:
            with st.spinner("Running QC checks... Please wait ⏳"):
                # Save uploaded files
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

                logging.info(f"📁 Uploaded Files → Rosco: {rosco_path}, BSR: {bsr_path}, Macro: {macro_path}")

                # === Detect Monitoring Period ===
                start_date, end_date = detect_period_from_rosco(rosco_path)
                df = load_bsr(bsr_path)
                st.info(f"📆 Monitoring Period: {start_date} → {end_date}")

                # === Clean Columns & Values ===
                df.columns = df.columns.str.strip().str.replace('\xa0', ' ', regex=True)
                df = df.applymap(lambda x: str(x).replace('\xa0', ' ').strip() if isinstance(x, str) else x)
                df.rename(columns={"Start(UTC)": "Start (UTC)", "End(UTC)": "End (UTC)"}, inplace=True)

                # === Run QC Checks ===
                st.write("🔍 Running QC checks...")

                df = period_check(df, start_date, end_date)
                df = completeness_check(df)
                df = overlap_duplicate_daybreak_check(df)
                df = program_category_check(bsr_path, df)
                df = duration_check(df)
                df = check_event_matchday_competition(df, bsr_path)
                df = market_channel_consistency_check(df, rosco_path, bsr_path)

                df = domestic_market_check(df, league_keyword="F24 Spain", debug=True)
                df = rates_and_ratings_check(df)
                df = duplicated_market_check(df, macro_path, league_keyword="F24 Spain", debug=True)
                df = country_channel_id_check(df)
                df = client_lstv_ott_check(df)
                df = rates_and_ratings_check(df)

                # === Save Output ===
                output_filename = f"QC_Result_{os.path.splitext(bsr_file.name)[0]}.xlsx"
                output_path = os.path.join(OUTPUT_FOLDER, output_filename)

                # Clean datetime columns
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

                # Save Excel
                with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name="QC Results")

                color_excel(output_path, df)
                generate_summary_sheet(output_path, df)

                # === Download Button ===
                with open(output_path, "rb") as f:
                    st.success("✅ QC completed successfully!")
                    st.download_button(
                        label="📥 Download QC Result Excel",
                        data=f,
                        file_name=output_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

        except Exception as e:
            logging.exception("❌ Error during QC run")
            st.error(f"❌ Error during QC: {str(e)}")