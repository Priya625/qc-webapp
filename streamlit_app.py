import streamlit as st
import pandas as pd
import os
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
    color_excel,
    generate_summary_sheet
)

# ----------------------- Folders -----------------------
UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "outputs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

st.set_page_config(page_title="QC Web App", layout="wide")
st.title("QC Web App")
st.markdown("Upload **ROSCO** and **BSR** Excel files to run QC and download annotated results.")

# ---------------------- File Upload ----------------------
rosco_file = st.file_uploader("Upload ROSCO file", type=["xlsx", "xls"])
bsr_file = st.file_uploader("Upload BSR file", type=["xlsx", "xls"])

# ---------------------- Run QC --------------------------
if st.button("Run QC"):

    if not rosco_file or not bsr_file:
        st.error("⚠️ Please upload both ROSCO and BSR files.")
    else:
        try:
            # Save uploaded files
            rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.name)
            bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.name)
            with open(rosco_path, "wb") as f:
                f.write(rosco_file.getbuffer())
            with open(bsr_path, "wb") as f:
                f.write(bsr_file.getbuffer())

            # --- Step 1: Detect period and load BSR ---
            start_date, end_date = detect_period_from_rosco(rosco_path)
            df = load_bsr(bsr_path)

            # --- Step 2: Run QC checks ---
            df = period_check(df, start_date, end_date)
            df = completeness_check(df)
            df = overlap_duplicate_daybreak_check(df)
            df = program_category_check(df)
            df = duration_check(df)
            df = check_event_matchday_competition(df, df_data=None, rosco_path=rosco_path)
            df = market_channel_program_duration_check(df, reference_df=None)
            df = domestic_market_coverage_check(df, reference_df=None)
            df = rates_and_ratings_check(df)
            df = duplicated_markets_check(df)

            # --- Step 3: Save output ---
            output_file = f"QC_Result_{os.path.splitext(bsr_file.name)[0]}.xlsx"
            output_path = os.path.join(OUTPUT_FOLDER, output_file)
            df.to_excel(output_path, index=False)

            # Add coloring and summary sheet
            color_excel(output_path, df)
            generate_summary_sheet(output_path, df)

            st.success("✅ QC completed successfully!")

            # Download button
            with open(output_path, "rb") as f:
                st.download_button(
                    "Download Annotated Excel",
                    f,
                    file_name=output_file,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        except Exception as e:
            st.error(f"❌ Error during QC: {e}")