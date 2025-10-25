from flask import Flask, render_template, request, send_file, redirect, url_for, flash
import os
import pandas as pd
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

app = Flask(__name__)
app.secret_key = "qc_secret_key"

UPLOAD_FOLDER = os.path.join(os.getcwd(), "uploads")
OUTPUT_FOLDER = os.path.join(os.getcwd(), "outputs")
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
        data_file = request.files.get("data_file")  # Optional Data sheet

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

        print("[INFO] Starting QC checks...")

        # Step 1: Detect period and load data
        start_date, end_date = detect_period_from_rosco(rosco_path)
        df = load_bsr(bsr_path)

        # Step 2: Run core QC checks
        df = period_check(df, start_date, end_date)
        df = completeness_check(df)
        df = overlap_duplicate_daybreak_check(df)
        df = program_category_check(df)
        df = duration_check(df)

        # Step 3: Event / Matchday / Competition QC Check
        print("\n--- Running Event / Matchday / Competition Check ---")
        if data_path:
            df_data = pd.read_excel(data_path)
            df = check_event_matchday_competition(df, df_data=df_data, rosco_path=rosco_path, debug_rows=20)
        else:
            df = check_event_matchday_competition(df, df_data=None, rosco_path=rosco_path, debug_rows=20)

        # Step 4: Market / Channel / Program / Duration Consistency Check
        print("\n--- Running Market / Channel / Program / Duration Consistency Check ---")
        if data_path:
            df = market_channel_program_duration_check(df, reference_df=pd.read_excel(data_path))
        else:
            df = market_channel_program_duration_check(df, reference_df=None)

        # Step 5: Domestic Market Coverage Check
        print("\n--- Running Domestic Market Coverage Check ---")
        if data_path:
            df = domestic_market_coverage_check(df, reference_df=pd.read_excel(data_path))
        else:
            df = domestic_market_coverage_check(df, reference_df=None)

        # Step 6: Rates and Ratings Check
        print("\n--- Running Rates and Ratings Check ---")
        df = rates_and_ratings_check(df)

        # Step 7: Comparison of Duplicated Markets
        print("\n--- Running Duplicated Markets Check ---")
        df = duplicated_markets_check(df)

        # Step 8: Save Output
        output_file = f"QC_Result_{os.path.splitext(bsr_file.filename)[0]}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_file)
        df.to_excel(output_path, index=False)

        # Step 9: Add coloring and summary sheet
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
    app.run(debug=True, port=5000)