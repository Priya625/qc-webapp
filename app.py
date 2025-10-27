from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import os
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows

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

app = Flask(__name__)
app.secret_key = "qc_secret_key"

BASE_DIR = os.getcwd()
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/run_qc", methods=["POST"])
def run_qc():
    try:
        rosco_file = request.files.get("rosco_file")
        bsr_file = request.files.get("bsr_file")
        data_file = request.files.get("data_file")  # optional

        if not rosco_file or not bsr_file:
            flash("⚠️ Please upload both Rosco and BSR files.")
            return redirect(url_for("index"))

        rosco_path = os.path.join(UPLOAD_FOLDER, rosco_file.filename)
        bsr_path = os.path.join(UPLOAD_FOLDER, bsr_file.filename)
        rosco_file.save(rosco_path)
        bsr_file.save(bsr_path)

        data_path = None
        if data_file:
            data_path = os.path.join(UPLOAD_FOLDER, data_file.filename)
            data_file.save(data_path)

        # Step 1: Period detection and BSR load
        start_date, end_date = detect_period_from_rosco(rosco_path)
        df = load_bsr(bsr_path)

        # Step 2: Core QC checks
        df = period_check(df, start_date, end_date)
        df = completeness_check(df)
        df = overlap_duplicate_daybreak_check(df)
        df = program_category_check(df)
        df = duration_check(df)

        # Step 3: Event / Matchday / Competition check
        if data_path:
            df_data = pd.read_excel(data_path)
            df = check_event_matchday_competition(df, df_data=df_data, rosco_path=rosco_path)
        else:
            df = check_event_matchday_competition(df, df_data=None, rosco_path=rosco_path)

        # Step 4: Market/Channel/Program/Dur consistency
        if data_path:
            df = market_channel_program_duration_check(df, reference_df=pd.read_excel(data_path))
        else:
            df = market_channel_program_duration_check(df, reference_df=None)

        # Step 5: Domestic coverage
        if data_path:
            df = domestic_market_coverage_check(df, reference_df=pd.read_excel(data_path))
        else:
            df = domestic_market_coverage_check(df, reference_df=None)

        # Step 6: Rates & Ratings
        df = rates_and_ratings_check(df)

        # Step 7: Duplicated markets
        df = duplicated_markets_check(df)

        # Step 8: Country & Channel ID check
        df = country_channel_id_check(df)

        # Step 9: Client / LSTV / OTT check
        df = client_lstv_ott_check(df)  # required_sources default to ['lstv', 'client', 'ott']

        # Step 10: Save output, color Excel, add summary
        output_file = f"QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)

        # Write DataFrame to Excel
        df.to_excel(output_path, index=False)

        # Apply coloring and add summary sheet
        color_excel(output_path, df)
        generate_summary_sheet(output_path, df)

        flash("✅ QC completed successfully!")
        return render_template("result.html", output_file=output_file)

    except Exception as e:
        flash(f"❌ Error during QC: {str(e)}")
        return redirect(url_for("index"))


@app.route("/download/<path:output_file>")
def download(output_file):
    file_path = os.path.join(OUTPUT_FOLDER, output_file)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        flash("⚠️ File not found.")
        return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)