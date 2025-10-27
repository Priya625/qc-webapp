import streamlit as st
import os
import time
import threading
import pandas as pd
from io import BytesIO

# ✅ Import your QC check functions
from qc_checks import (
    detect_period_from_rosco,
    load_bsr,
    period_check,
    completeness_check,
    overlap_duplicate_daybreak_check,
    program_category_check,
    duration_check,
    check_event_matchday_competition,
    market_channel_program_duration_check,
    domestic_market_coverage_check,
    rates_and_ratings_check,
    duplicated_markets_check,
    country_channel_id_check,
    client_lstv_ott_check,
    color_excel,
    generate_summary_sheet,
)

# -------------------- ⚙️ Folder setup --------------------
BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------- 🧹 Cleanup Function --------------------
def cleanup_old_files(folder_path, max_age_minutes=30):
    """
    Deletes files older than max_age_minutes from the specified folder.
    """
    now = time.time()
    max_age_seconds = max_age_minutes * 60

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            file_age = now - os.path.getmtime(file_path)
            if file_age > max_age_seconds:
                try:
                    os.remove(file_path)
                    print(f"🧹 Deleted old file: {file_path}")
                except Exception as e:
                    print(f"⚠️ Error deleting {file_path}: {e}")

# -------------------- 🔄 Background Cleanup --------------------
def start_background_cleanup():
    """
    Starts a background thread that cleans old files every 5 minutes.
    """
    def run_cleanup():
        while True:
            cleanup_old_files(UPLOAD_FOLDER, max_age_minutes=30)
            cleanup_old_files(OUTPUT_FOLDER, max_age_minutes=30)
            time.sleep(300)  # every 5 minutes

    thread = threading.Thread(target=run_cleanup, daemon=True)
    thread.start()

# Start cleanup as soon as the app starts
start_background_cleanup()

# -------------------- 🌐 Streamlit UI --------------------
st.set_page_config(page_title="QC Automation App", layout="wide")
st.title("🧾 Automated QC Checker")

st.markdown("""
Upload your **Rosco**, **BSR**, and (optional) **Client Data file** below to run automated QC checks.  
Results will be generated as an Excel file for download.
""")

# -------------------- 📁 File Upload Section --------------------
rosco_file = st.file_uploader("📘 Upload Rosco File (.xlsx)", type=["xlsx"], key="rosco")
bsr_file = st.file_uploader("📗 Upload BSR File (.xlsx)", type=["xlsx"], key="bsr")
data_file = st.file_uploader("📙 Upload Optional Data File (.xlsx)", type=["xlsx"], key="data")

# -------------------- ▶️ Run Button --------------------
if st.button("🚀 Run QC Checks"):
    if not rosco_file or not bsr_file:
        st.error("⚠️ Please upload both Rosco and BSR files before running QC.")
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

                data_path = None
                if data_file:
                    data_path = os.path.join(UPLOAD_FOLDER, data_file.name)
                    with open(data_path, "wb") as f:
                        f.write(data_file.getbuffer())

                # -------------------- 🧠 Run QC Pipeline --------------------
                start_date, end_date = detect_period_from_rosco(rosco_path)
                df = load_bsr(bsr_path)

                df = period_check(df, start_date, end_date)
                df = completeness_check(df)
                df = overlap_duplicate_daybreak_check(df)
                df = program_category_check(df)
                df = duration_check(df)

                if data_path:
                    df_data = pd.read_excel(data_path)
                    df = check_event_matchday_competition(df, df_data=df_data, rosco_path=rosco_path)
                    df = market_channel_program_duration_check(df, reference_df=df_data)
                    df = domestic_market_coverage_check(df, reference_df=df_data)
                else:
                    df = check_event_matchday_competition(df, df_data=None, rosco_path=rosco_path)
                    df = market_channel_program_duration_check(df, reference_df=None)
                    df = domestic_market_coverage_check(df, reference_df=None)

                df = rates_and_ratings_check(df)
                df = duplicated_markets_check(df)
                df = country_channel_id_check(df)
                df = client_lstv_ott_check(df)

                # -------------------- 📊 Output Generation --------------------
                output_filename = f"QC_Result_{os.path.splitext(bsr_file.name)[0]}.xlsx"
                output_path = os.path.join(OUTPUT_FOLDER, output_filename)
                df.to_excel(output_path, index=False)

                color_excel(output_path, df)
                generate_summary_sheet(output_path, df)

                # -------------------- ✅ Download Button --------------------
                with open(output_path, "rb") as f:
                    st.success("✅ QC completed successfully!")
                    st.download_button(
                        label="📥 Download QC Result Excel",
                        data=f,
                        file_name=output_filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

        except Exception as e:
            st.error(f"❌ Error during QC processing: {str(e)}")