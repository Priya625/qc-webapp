import re
import os
import pandas as pd
import numpy as np
import logging
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows

# === Setup logging ===
logging.basicConfig(
    filename="qc_debug.log",        # File where logs will be saved
    level=logging.DEBUG,            # Capture detailed logs
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DATE_FORMAT = "%Y-%m-%d"

GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
HEADER_FILL = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")


# ----------------------------- Helpers -----------------------------
def _find_column(df, candidates):
    """
    Case-insensitive lookup for a column in df.columns.
    candidates: list of possible header names (strings).
    Returns first matching actual column name or None.
    """
    lower_map = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand is None:
            continue
        key = cand.lower().strip()
        if key in lower_map:
            return lower_map[key]
    return None


def _is_present(val):
    """
    Treat numeric values (including 0) as present.
    For strings: strip whitespace and consider 'nan'/'none' as absent.
    None/NaN -> False.
    """
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except Exception:
        pass
    # Numeric -> present (including 0)
    if isinstance(val, (int, float)) and not (isinstance(val, float) and pd.isna(val)):
        return True
    s = str(val).strip()
    if s == "":
        return False
    if s.lower() in ("nan", "none"):
        return False
    return True


# ----------------------------- 1️⃣ Detect Monitoring Period -----------------------------
def detect_period_from_rosco(rosco_path):
    """
    Attempts to find 'Monitoring Period' row anywhere in the Rosco file and extract two dates (YYYY-MM-DD).
    Returns (start_date, end_date) as pandas.Timestamp.
    Raises ValueError if not found or parsed.
    """
    x = pd.read_excel(rosco_path, header=None, dtype=str)
    # Flatten to strings and search for "Monitoring" phrase
    combined_text = x.fillna("").astype(str).apply(lambda row: " ".join(row.values), axis=1)
    match_rows = combined_text[combined_text.str.contains("Monitoring Period", case=False, na=False)]
    if match_rows.empty:
        # fallback: search for "Monitoring Periods" or "Monitoring period"
        match_rows = combined_text[combined_text.str.contains("Monitoring Periods|Monitoring period", case=False, na=False)]
    if match_rows.empty:
        # final fallback: search entire sheet for date patterns and pick earliest two if found
        all_text = " ".join(combined_text.tolist())
        found = re.findall(r"\d{4}-\d{2}-\d{2}", all_text)
        if len(found) >= 2:
            start_date = pd.to_datetime(found[0], format=DATE_FORMAT)
            end_date = pd.to_datetime(found[1], format=DATE_FORMAT)
            return start_date, end_date
        raise ValueError("Could not find 'Monitoring Period' text in Rosco file.")

    text_row = match_rows.iloc[0]
    found = re.findall(r"\d{4}-\d{2}-\d{2}", text_row)
    if len(found) >= 2:
        start_date = pd.to_datetime(found[0], format=DATE_FORMAT)
        end_date = pd.to_datetime(found[1], format=DATE_FORMAT)
        return start_date, end_date

    # if dates not in YYYY-MM-DD, try other common formats (dd/mm/yyyy etc.)
    found_alt = re.findall(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", text_row)
    if len(found_alt) >= 2:
        # try parsing with pandas
        try:
            start_date = pd.to_datetime(found_alt[0], dayfirst=False, errors="coerce")
            end_date = pd.to_datetime(found_alt[1], dayfirst=False, errors="coerce")
            if pd.notna(start_date) and pd.notna(end_date):
                return start_date, end_date
        except Exception:
            pass

    raise ValueError("Could not parse monitoring period dates from Rosco file.")


# ----------------------------- 2️⃣ Load BSR -----------------------------
def detect_header_row(bsr_path):
    df_sample = pd.read_excel(bsr_path, header=None, nrows=200)
    for i, row in df_sample.iterrows():
        row_str = " ".join(row.dropna().astype(str).tolist()).lower()
        if "region" in row_str and "market" in row_str and "broadcaster" in row_str:
            return i
        if "date" in row_str and ("utc" in row_str or "gmt" in row_str):
            return i
    raise ValueError("Could not detect header row in BSR file.")


def load_bsr(bsr_path):
    header_row = detect_header_row(bsr_path)
    df = pd.read_excel(bsr_path, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ----------------------------- 3️⃣ Period Check -----------------------------
def period_check(df, start_date, end_date):
    date_col = next((c for c in df.columns if "date" in str(c).lower()), None)
    if not date_col:
        df["Within_Period_OK"] = True
        df["Within_Period_Remark"] = ""
        return df
    df["Date_checked"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df["Within_Period_OK"] = df["Date_checked"].between(start_date.date(), end_date.date())
    df["Within_Period_Remark"] = df["Within_Period_OK"].apply(lambda x: "" if x else "Date outside monitoring period")
    return df


# ----------------------------- 4️⃣ Completeness Check -----------------------------
def completeness_check(df):
    """
    ✅ Completeness Check (Optimized with Correct Audience Logic)
    -------------------------------------------------
    Checks data completeness in BSR file:
    - Mandatory: TV Channel, Channel ID, Match Day, Source
    - Audience: exactly one of Aud. Estimates / Aud. Metered must be filled (0 counts as filled)
    - Type of Program: if Live/Repeat/Delayed → Home & Away Teams required
    """

    required_columns = {
        "tv_channel": ["TV Channel", "TV-Channel", "Channel", "TV Channel "],
        "channel_id": ["Channel ID", "ChannelID", "Channel Id"],
        "type_of_program": ["Type of Program", "Type of programme", "Type of program"],
        "match_day": ["Matchday", "Match Day", "Matchday "],
        "home_team": ["Home Team", "HomeTeam", "Home"],
        "away_team": ["Away Team", "AwayTeam", "Away"],
        "aud_estimates": ["Aud. Estimates ['000s]", "Audience Estimates", "Aud Estimates"],
        "aud_metered": ["Aud Metered (000s) 3+", "Audience Metered", "Aud. Metered (000s) 3+"],
        "source": ["Source", "AudienceSource", "Audience Source", "Audience_Source"]
    }

    # --- Map logical names to actual columns (case-insensitive)
    lower_to_actual = {col.lower(): col for col in df.columns}
    colmap = {}
    for key, opts in required_columns.items():
        found = next((lower_to_actual.get(opt.lower()) for opt in opts if opt.lower() in lower_to_actual), None)
        colmap[key] = found

    # --- Fallback for "Audience Source"
    if not colmap.get("source"):
        for col in df.columns:
            if "audience source" in col.lower():
                colmap["source"] = col
                break

    # --- Helper to detect presence (0 and 0.00 count as filled)
    def is_present(val):
        if val is None:
            return False
        if isinstance(val, (int, float)):
            # NaN is empty, 0 and 0.0 are valid
            return not pd.isna(val)
        s = str(val).replace("\xa0", "").strip().lower()
        return not (s == "" or s in ["nan", "none"])

    # --- Initialize result columns
    df["Completeness_OK"] = True
    df["Completeness_Remark"] = "All key fields present"

    live_types = {"live", "repeat", "delayed"}
    relaxed_types = {"highlights", "magazine", "support", "magazine and support"}

    # --- Iterate rows
    for idx, row in df.iterrows():
        missing = []

        # 1️⃣ Mandatory Fields
        for logical, display in [("tv_channel", "TV Channel"), ("channel_id", "Channel ID"),
                                 ("match_day", "Match Day"), ("source", "Source")]:
            colname = colmap.get(logical)
            if colname is None:
                missing.append(f"{display} (column not found)")
            elif not is_present(row.get(colname)):
                missing.append(display)

        # 2️⃣ Audience Logic (EXACTLY ONE should be filled)
        aud_est_col = colmap.get("aud_estimates")
        aud_met_col = colmap.get("aud_metered")

        if not aud_est_col and not aud_met_col:
            missing.append("Audience (Estimates/Metered) (columns not found)")
        else:
            est_val = row.get(aud_est_col) if aud_est_col else None
            met_val = row.get(aud_met_col) if aud_met_col else None

            est_present = is_present(est_val)
            met_present = is_present(met_val)

            # --- Exclusive check ---
            if not est_present and not met_present:
                missing.append("Both Audience fields are empty")
            elif est_present and met_present:
                missing.append("Both Audience fields are filled")

        # 3️⃣ Type-based (Home/Away)
        type_col = colmap.get("type_of_program")
        prog_type = str(row.get(type_col) or "").strip().lower() if type_col else ""
        home_col, away_col = colmap.get("home_team"), colmap.get("away_team")

        if prog_type in live_types:
            if not home_col:
                missing.append("Home Team (column not found)")
            elif not is_present(row.get(home_col)):
                missing.append("Home Team")

            if not away_col:
                missing.append("Away Team (column not found)")
            elif not is_present(row.get(away_col)):
                missing.append("Away Team")

        elif prog_type not in relaxed_types:
            if home_col and not is_present(row.get(home_col)):
                missing.append("Home Team")
            if away_col and not is_present(row.get(away_col)):
                missing.append("Away Team")

        # 4️⃣ Final result
        if missing:
            df.at[idx, "Completeness_OK"] = False
            df.at[idx, "Completeness_Remark"] = "; ".join(missing)

    return df

# ----------------------------- 5️⃣ Overlap / Duplicate / Day Break -----------------------------
def overlap_duplicate_daybreak_check(df):
    """
    ✅ Performs three QC checks (non-destructive):
      1. Overlap check – consecutive programs overlap in time.
      2. Duplicate check – exact duplicate rows for same Channel/Date/Time.
      3. Daybreak check – program continuation breaks incorrectly across days.

    Returns:
      Original DataFrame + new QC columns:
        Overlap_OK, Overlap_Remark,
        Duplicate_OK, Duplicate_Remark,
        Daybreak_OK, Daybreak_Remark
    """

    if df is None or df.empty:
        return df

    df_in = df.copy(deep=True)

    # -------- Helper to find columns (case-insensitive) --------
    def find_col(sub):
        for c in df_in.columns:
            if sub.lower() in str(c).lower():
                return c
        return None

    col_channel = find_col("channel") or "TV Channel"
    col_channel_id = find_col("channel id") or "Channel ID"
    col_date = find_col("date") or "Date (UTC/GMT)"
    col_start = find_col("start") or "Start (UTC)"
    col_end = find_col("end") or "End (UTC)"
    col_pay = find_col("pay") or "Pay/Free TV"
    col_combined = find_col("combined")

    # -------- Parse time safely --------
    df_in["_qc_start_dt"] = pd.to_datetime(df_in[col_start], format="%H:%M:%S", errors="coerce") if col_start in df_in else pd.NaT
    df_in["_qc_end_dt"] = pd.to_datetime(df_in[col_end], format="%H:%M:%S", errors="coerce") if col_end in df_in else pd.NaT
    df_in["_orig_index"] = df_in.index

    # -------- Sort for sequential checks --------
    sort_cols = [c for c in [col_channel, col_date, "_qc_start_dt"] if c in df_in.columns]
    df_work = df_in.sort_values(by=sort_cols, na_position="last").reset_index(drop=True)

    # =====================================================
    # 1️⃣ OVERLAP CHECK
    # =====================================================
    overlap_ok = pd.Series(True, index=df_work.index)
    overlap_remark = pd.Series("", index=df_work.index)

    try:
        prev_end = df_work["_qc_end_dt"].shift(1)
        same_channel = df_work[col_channel].eq(df_work[col_channel].shift(1))
        same_date = df_work[col_date].eq(df_work[col_date].shift(1))
        is_ott = df_work[col_pay].astype(str).str.lower().str.contains("ott|internet|www", na=False) if col_pay in df_work else False

        overlap_mask = same_channel & same_date & (~is_ott) & df_work["_qc_start_dt"].notna() & prev_end.notna() & (df_work["_qc_start_dt"] < prev_end)
        overlap_ok.loc[overlap_mask] = False
        overlap_remark.loc[overlap_mask] = "Overlap detected between consecutive programs"
    except Exception as e:
        print(f"⚠️ Overlap logic failed: {e}")

    # =====================================================
    # 2️⃣ DUPLICATE CHECK
    # =====================================================
    duplicate_ok = pd.Series(True, index=df_work.index)
    duplicate_remark = pd.Series("", index=df_work.index)

    try:
        dup_cols = [c for c in [col_channel, col_date, col_start, col_end] if c in df_in.columns]
        if dup_cols:
            dup_mask = df_in.duplicated(subset=dup_cols, keep=False)
            dup_mask_work = df_work["_orig_index"].isin(df_in[dup_mask].index)
            duplicate_ok.loc[dup_mask_work] = False
            duplicate_remark.loc[dup_mask_work] = "Duplicate row found"
    except Exception as e:
        print(f"⚠️ Duplicate logic failed: {e}")

    # =====================================================
    # 3️⃣ DAYBREAK CHECK
    # =====================================================
    daybreak_ok = pd.Series(True, index=df_work.index)
    daybreak_remark = pd.Series("", index=df_work.index)

    try:
        for i in range(1, len(df_work)):
            curr, prev = df_work.iloc[i], df_work.iloc[i - 1]
            same_channel_val = (col_channel in df_work and curr[col_channel] == prev[col_channel])
            same_channel_id = (col_channel_id in df_work and curr[col_channel_id] == prev[col_channel_id])
            same_combined = (col_combined in df_work and curr.get(col_combined) == prev.get(col_combined))

            if same_channel_val and same_channel_id and same_combined:
                if pd.notna(prev["_qc_end_dt"]) and pd.notna(curr["_qc_start_dt"]):
                    gap = (curr["_qc_start_dt"] - prev["_qc_end_dt"]).total_seconds() / 60
                    if gap < 0 or gap > 2:
                        daybreak_ok.iat[i] = False
                        daybreak_remark.iat[i] = "Invalid continuation gap"
            else:
                if pd.notna(prev["_qc_end_dt"]) and pd.notna(curr["_qc_start_dt"]) and curr["_qc_start_dt"].day != prev["_qc_end_dt"].day:
                    daybreak_ok.iat[i] = False
                    daybreak_remark.iat[i] = "Continuation across daybreak"
    except Exception as e:
        print(f"⚠️ Daybreak logic failed: {e}")

    # =====================================================
    # Map Results Back
    # =====================================================
    res = pd.DataFrame({
        "Overlap_OK": overlap_ok,
        "Overlap_Remark": overlap_remark,
        "Duplicate_OK": duplicate_ok,
        "Duplicate_Remark": duplicate_remark,
        "Daybreak_OK": daybreak_ok,
        "Daybreak_Remark": daybreak_remark
    })

    res["_orig_index"] = df_work["_orig_index"].values
    res.set_index("_orig_index", inplace=True)
    df_out = df_in.join(res, how="left")

    return df_out


# ----------------------------- 6️⃣ Program Category Check -----------------------------
def program_category_check(bsr_path, df):
    """
    ✅ Program Category Check (Optimized for Flask Integration)
    - bsr_path: Path to the BSR Excel file
    - df: DataFrame already loaded from BSR (main sheet)
    - Fixture list sheet = 'Fixture List' or 'Fixtures List' (case-insensitive)
    """

    # --- Load Excel file ---
    xl = pd.ExcelFile(bsr_path)
    sheet_names = xl.sheet_names

    # Identify fixture list sheet (case-insensitive)
    fixture_sheet = next((s for s in sheet_names if "fixture" in s.lower()), None)

    # --- If fixture sheet is missing ---
    if not fixture_sheet:
        df["Program_Category_Expected"] = "unknown"
        df["Program_Category_Actual"] = df.get("Type of Program", "")
        df["Program_Category_OK"] = False
        df["Program_Category_Remark"] = "Fixture list sheet missing"
        return df

    # --- Load Fixture Sheet ---
    df_fix = xl.parse(fixture_sheet)
    df_fix.columns = df_fix.columns.map(str)  # 🔧 ensure all columns are strings
    df.columns = df.columns.map(str)

    # --- Helper to find columns dynamically ---
    def find_col(df_, keywords):
        for col in df_.columns:
            if isinstance(col, str) and any(k in col.lower() for k in keywords):
                return col
        return None

    # Identify BSR columns
    col_home_bsr  = find_col(df, ["home"])
    col_away_bsr  = find_col(df, ["away"])
    col_date_bsr  = find_col(df, ["date (utc)", "date"])
    col_start_bsr = find_col(df, ["start (utc)", "start"])
    col_end_bsr   = find_col(df, ["end (utc)", "end"])
    col_progtype  = find_col(df, ["type"])
    col_desc      = find_col(df, ["program", "description", "title"])

    # Identify Fixture columns
    col_home_fix  = find_col(df_fix, ["home"])
    col_away_fix  = find_col(df_fix, ["away"])
    col_date_fix  = find_col(df_fix, ["date"])
    col_start_fix = find_col(df_fix, ["start"])
    col_end_fix   = find_col(df_fix, ["end"])

    # --- Convert to datetime ---
    for df_, cols in [(df, [col_start_bsr, col_end_bsr, col_date_bsr]),
                      (df_fix, [col_start_fix, col_end_fix, col_date_fix])]:
        for c in cols:
            if c and c in df_.columns:
                df_[c] = pd.to_datetime(df_[c], errors="coerce")

    # --- Prepare result containers ---
    expected, actual, ok_list, remark = [], [], [], []

    # --- Row-wise Comparison ---
    for _, row in df.iterrows():
        actual_type = str(row.get(col_progtype, "")).strip().lower()
        desc = str(row.get(col_desc, "")).strip().lower()
        home = str(row.get(col_home_bsr, "")).strip().lower()
        away = str(row.get(col_away_bsr, "")).strip().lower()
        date_bsr = row.get(col_date_bsr)
        start_bsr = row.get(col_start_bsr)
        end_bsr = row.get(col_end_bsr)

        exp_type, ok, note = "unknown", True, "OK"

        # --- Keyword-based rules ---
        if any(k in desc for k in ["pre", "studio", "interview", "analysis"]):
            exp_type = "magazine/support"
        elif any(k in desc for k in ["highlight", "hits", "recap", "summary", "overview"]):
            exp_type = "highlights"
        else:
            # --- Match with Fixture ---
            fix_match = df_fix[
                (df_fix[col_home_fix].astype(str).str.lower() == home) &
                (df_fix[col_away_fix].astype(str).str.lower() == away)
            ]

            # Match by date if available
            if col_date_fix and not pd.isna(date_bsr):
                fix_match = fix_match[
                    pd.to_datetime(df_fix[col_date_fix], errors="coerce").dt.date ==
                    pd.to_datetime(date_bsr).date()
                ]

            if fix_match.empty:
                exp_type, ok, note = "unknown", False, "No matching fixture found"
            else:
                fix_row = fix_match.iloc[0]
                start_fix, end_fix = fix_row.get(col_start_fix), fix_row.get(col_end_fix)

                if pd.isna(start_fix) or pd.isna(end_fix) or pd.isna(start_bsr) or pd.isna(end_bsr):
                    exp_type, ok, note = "unknown", False, "Invalid or missing time"
                else:
                    # --- Compare times ---
                    start_diff = abs((start_bsr - start_fix).total_seconds()) / 60
                    end_diff   = abs((end_bsr - end_fix).total_seconds()) / 60
                    duration   = abs((end_bsr - start_bsr).total_seconds()) / 60

                    # --- Duration logic ---
                    if (90 <= duration <= 150) or (start_diff <= 30 and end_diff <= 30):
                        exp_type = "live"
                    elif start_diff <= 70 or end_diff <= 70:
                        exp_type = "repeat"
                    else:
                        exp_type = "highlights"

        # --- Final comparison ---
        ok = (exp_type == actual_type)
        note = "OK" if ok else f"Expected '{exp_type}', found '{actual_type}'"

        expected.append(exp_type)
        actual.append(actual_type)
        ok_list.append(ok)
        remark.append(note)

    # --- Assign to df and return ---
    df["Program_Category_Expected"] = expected
    df["Program_Category_Actual"] = actual
    df["Program_Category_OK"] = ok_list
    df["Program_Category_Remark"] = remark

    return df


# ----------------------------- 7️⃣ Duration Check -----------------------------
def duration_check(df):
    """
    Duration Check (Hybrid Keyword & Duration Logic):
    1. Combines Date (UTC/GMT) + Start(UTC)/End(UTC)
    2. Calculates actual duration in minutes
    3. Validates by program type using a logic waterfall:
       - Keywords for Repeat
       - Keywords AND Duration for Magazine/Highlights
       - Duration for Live
    4. Enforces BSA <= 180 mins rule for Live/Repeat
    """

    import pandas as pd
    import logging

    logging.info("🚀 Starting Hybrid Duration Check...")

    # --- Identify relevant columns (Using exact headers) ---
    col_date = "Date (UTC/GMT)"
    col_start = "Start (UTC)"
    col_end = "End (UTC)"
    col_type = "Type of program"
    col_desc = "Program Title" # Using this for description keywords
    col_source = "Source"

    # --- Check if all required columns exist ---
    required_cols = [col_date, col_start, col_end, col_type, col_desc, col_source]
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"❌ Missing required column: '{col}'. Stopping duration check.")
            # Add error columns so the script doesn't fail later
            df["Duration_OK"] = False
            df["Duration_Remark"] = f"Missing required column: {col}"
            df["Duration_Mins"] = None
            return df

    logging.info("✅ All required columns found.")

    # Add the 3 result columns
    df["Duration_OK"] = False
    df["Duration_Remark"] = ""
    df["Duration_Mins"] = None

    # --- Combine date + time for UTC columns ---
    try:
        df["_StartDT"] = pd.to_datetime(df[col_date].astype(str) + " " + df[col_start].astype(str), errors="coerce", dayfirst=True, utc=True)
        df["_EndDT"] = pd.to_datetime(df[col_date].astype(str) + " " + df[col_end].astype(str), errors="coerce", dayfirst=True, utc=True)
    except Exception as e:
        logging.error(f"⚠️ Error combining datetime columns: {e}")

    # --- Compute duration ---
    df["Duration_Mins"] = (df["_EndDT"] - df["_StartDT"]).dt.total_seconds() / 60

    # --- Define Keyword Lists ---
    MAG_KEYWORDS = ["pre", "studio", "interview", "analysis"]
    HI_KEYWORDS = ["highlight", "hits", "recap", "summary", "overview"]
    REPEAT_KEYWORDS = ["repeat", "replay", "delayed"]

    for i, row in df.iterrows():
        dur = row["Duration_Mins"]
        actual_type = str(row.get(col_type, "")).strip().lower()
        desc = str(row.get(col_desc, "")).strip().lower()
        source = str(row.get(col_source, "")).strip().lower()

        expected_type = "unknown" # This is our "expected" type

        # 1. Validate time
        if pd.isna(dur):
            df.at[i, "Duration_Remark"] = "Invalid or missing Start/End UTC"
            continue

        # 2. Special BSA Rule (This is a priority failure check)
        if "bsa" in source and (actual_type in ["live", "repeat"]) and dur > 180:
            df.at[i, "Duration_OK"] = False
            df.at[i, "Duration_Remark"] = "BSA Live/Repeat > 180 mins (Invalid)"
            continue

        # 3. --- Start Logic Waterfall to find 'expected_type' ---
        
        # Rule 1: Check for Repeat Keywords FIRST (highest priority)
        # We check both the description AND the actual type column for 'repeat'
        if any(k in desc for k in REPEAT_KEYWORDS) or any(k in actual_type for k in REPEAT_KEYWORDS):
            expected_type = "repeat"
        
        # Rule 2: Check for Magazine Keywords + Duration
        elif any(k in desc for k in MAG_KEYWORDS) and (10 <= dur <= 40):
            expected_type = "magazine/support"
        
        # Rule 3: Check for Highlights Keywords + Duration
        elif any(k in desc for k in HI_KEYWORDS) and (10 <= dur <= 40):
            expected_type = "highlights"
        
        # Rule 4: Check for Live Duration (Fallback)
        elif 90 <= dur <= 150:
            expected_type = "live"
            
        # 4. --- Final Comparison ---
        # Now we compare the 'expected_type' we just found to the 'actual_type'
        
        # Handle 'delayed' being a 'repeat'
        if expected_type == "repeat" and actual_type == "delayed":
             df.at[i, "Duration_OK"] = True
             df.at[i, "Duration_Remark"] = "OK"
        
        elif expected_type == actual_type:
            df.at[i, "Duration_OK"] = True
            df.at[i, "Duration_Remark"] = "OK"
        
        elif expected_type == "unknown":
            df.at[i, "Duration_OK"] = False # Failed
            df.at[i, "Duration_Remark"] = f"Unclassified: Found '{actual_type}' with {dur:.0f} min duration"
        
        else: # Mismatch
            df.at[i, "Duration_OK"] = False # Failed
            df.at[i, "Duration_Remark"] = f"Expected '{expected_type}', found '{actual_type}'"

    # Clean up temporary columns
    df = df.drop(columns=["_StartDT", "_EndDT"], errors="ignore")

    logging.info("✅ Hybrid Duration Check Completed.")
    return df

# 8️⃣ Event / Matchday / Competition Check
def check_event_matchday_competition(df, bsr_path):
    """
    ✅ Event–Matchday–Fixture consistency check
    - Reads 'Fixture List' sheet from the same BSR Excel file.
    - Uses 'Competition' as 'Event' if 'Event' column missing.
    - Compares Event, Home Team, Away Team, and Matchday.
    - Ignores the 'Competition' column in main BSR sheet.
    """

    logging.info("Starting Event / Matchday / Fixture consistency check...")

    df["Event_Matchday_OK"] = True
    df["Event_Matchday_Remark"] = "OK"

    df.columns = [c.strip() for c in df.columns]

    # Load fixture list
    fixture_df = None
    try:
        excel_file = pd.ExcelFile(bsr_path)
        fixture_sheet = None
        for sheet in excel_file.sheet_names:
            if "fixture" in sheet.lower():
                fixture_sheet = sheet
                break
        if fixture_sheet:
            fixture_df = excel_file.parse(fixture_sheet)
            fixture_df.columns = [c.strip() for c in fixture_df.columns]
            logging.info(f"📄 Loaded fixture sheet: '{fixture_sheet}' with {len(fixture_df)} rows.")
        else:
            logging.warning("⚠️ No sheet containing 'fixture' found.")
    except Exception as e:
        logging.error(f"❌ Error loading fixture list: {e}")

    if fixture_df is not None:
        # Use Competition as Event if Event not present
        if "Event" not in fixture_df.columns and "Competition" in fixture_df.columns:
            fixture_df.rename(columns={"Competition": "Event"}, inplace=True)

        # Normalize data
        for col in ["Event", "Home Team", "Away Team", "Matchday"]:
            if col in fixture_df.columns:
                fixture_df[col] = fixture_df[col].astype(str).str.strip().str.lower()

    # Main comparison
    for i, row in df.iterrows():
        try:
            event = str(row.get("Event", "")).strip().lower()
            home = str(row.get("Home Team", "")).strip().lower()
            away = str(row.get("Away Team", "")).strip().lower()
            matchday = str(row.get("Matchday", "")).strip().lower()

            if not event or not home or not away or not matchday:
                df.at[i, "Event_Matchday_OK"] = False
                df.at[i, "Event_Matchday_Remark"] = "Missing event/home/away/matchday"
                continue

            if fixture_df is not None:
                match = fixture_df[
                    (fixture_df["Event"] == event)
                    & (fixture_df["Home Team"] == home)
                    & (fixture_df["Away Team"] == away)
                    & (fixture_df["Matchday"] == matchday)
                ]

                if match.empty:
                    df.at[i, "Event_Matchday_OK"] = False
                    df.at[i, "Event_Matchday_Remark"] = "No matching fixture found"
                else:
                    df.at[i, "Event_Matchday_OK"] = True
                    df.at[i, "Event_Matchday_Remark"] = "OK"
            else:
                df.at[i, "Event_Matchday_OK"] = True
                df.at[i, "Event_Matchday_Remark"] = "OK (fixture list missing)"
        except Exception as e:
            df.at[i, "Event_Matchday_OK"] = False
            df.at[i, "Event_Matchday_Remark"] = f"Error: {e}"

    logging.info("✅ Event / Matchday / Fixture consistency check completed.")
    return df

# -----------------------------------------------------------
def market_channel_consistency_check(df_bsr, rosco_path=None, bsr_path=None):
    """
    ✅ Market & Channel Consistency Check

    Compares Market + TV-Channel in BSR with ChannelCountry + ChannelName in ROSCO.

    Rules:
    - Ignore the 'General Information' sheet in ROSCO.
    - Normalize channel names ONLY in ROSCO (remove brackets, hyphens, etc.).
    - Check for missing or invalid Market/Channel in BSR.
    - Check for missing Program Description (optional).
    - Optional Spain fixture list check (if 'Fixture' sheet exists in BSR).

    Output columns:
        Market_Channel_Consistency_OK
        Program_Description_OK
        Market_Channel_Program_Remark
    """
    logging.info("🔍 Starting Market & Channel Consistency Check...")

    # ----------------- Normalization helper for ROSCO -----------------
    def normalize_channel(name):
        if pd.isna(name):
            return ""
        s = str(name)
        s = re.sub(r"\(.*?\)|\[.*?\]", "", s)  # remove bracketed text
        s = re.split(r"[-–—]", s)[0]           # remove after hyphen/dash
        s = re.sub(r"[^0-9a-zA-Z\s]", " ", s)  # keep alphanumeric + spaces
        s = re.sub(r"\s+", " ", s).strip().lower()
        return s

    # ----------------- Load ROSCO reference sheet -----------------
    rosco_df = None
    if rosco_path:
        try:
            xls = pd.ExcelFile(rosco_path)
            # Find the sheet other than General Information
            sheet_name = next((s for s in xls.sheet_names if "general" not in s.lower()), None)
            if sheet_name:
                rosco_df = xls.parse(sheet_name)
                logging.info(f"📄 Loaded ROSCO reference sheet: {sheet_name}")
            else:
                logging.warning("⚠️ No valid sheet found in ROSCO (only General Information).")
        except Exception as e:
            logging.error(f"❌ Error loading ROSCO file: {e}")
            return df_bsr

    # ----------------- Build valid (Market, Channel) pairs from ROSCO -----------------
    valid_pairs = set()
    if rosco_df is not None:
        if {"ChannelCountry", "ChannelName"}.issubset(rosco_df.columns):
            for _, row in rosco_df.iterrows():
                market = str(row["ChannelCountry"]).strip().lower()
                channel = normalize_channel(row["ChannelName"])
                if market and channel:
                    valid_pairs.add((market, channel))
            logging.info(f"✅ Loaded {len(valid_pairs)} valid Market+Channel pairs from ROSCO.")
        else:
            logging.warning("⚠️ 'ChannelCountry' or 'ChannelName' columns not found in ROSCO sheet.")

    # ----------------- Optional: Load fixture list for Spain check -----------------
    expected_spain_fixtures = None
    if bsr_path:
        try:
            xls = pd.ExcelFile(bsr_path)
            fixture_sheet = next((s for s in xls.sheet_names if "fixture" in s.lower()), None)
            if fixture_sheet:
                fix_df = xls.parse(fixture_sheet)
                fix_df.columns = [c.strip().lower() for c in fix_df.columns]
                # crude heuristic: if sheet looks like home/away table
                if len(fix_df.columns) >= 2:
                    fix_df["_fixture_key"] = fix_df.iloc[:, 0].astype(str).str.lower() + " vs " + fix_df.iloc[:, 1].astype(str).str.lower()
                    expected_spain_fixtures = fix_df["_fixture_key"].nunique()
                    logging.info(f"⚽ Fixture sheet '{fixture_sheet}' found: {expected_spain_fixtures} unique matches.")
        except Exception as e:
            logging.warning(f"⚠️ Error reading fixture list from BSR: {e}")

    # ----------------- Prepare result columns -----------------
    df_bsr["Market_Channel_Consistency_OK"] = True
    df_bsr["Program_Description_OK"] = True
    df_bsr["Market_Channel_Program_Remark"] = "OK"

    # ----------------- Validate each row in BSR -----------------
    for idx, row in df_bsr.iterrows():
        remarks = []
        market = str(row.get("Market", "")).strip().lower()
        channel = str(row.get("TV-Channel", "")).strip()
        prog = str(row.get("Program Description", "")).strip() if "Program Description" in df_bsr.columns else ""

        # check program description
        if not prog or prog.lower() in ["-", "nan", "none"]:
            df_bsr.at[idx, "Program_Description_OK"] = False
            remarks.append("Missing program description")

        # check market-channel pair
        if not market or not channel:
            df_bsr.at[idx, "Market_Channel_Consistency_OK"] = False
            remarks.append("Missing market or channel")
        elif valid_pairs:
            if (market, normalize_channel(channel)) not in valid_pairs:
                df_bsr.at[idx, "Market_Channel_Consistency_OK"] = False
                remarks.append("Market+Channel not found in ROSCO")

        df_bsr.at[idx, "Market_Channel_Program_Remark"] = "; ".join(remarks) if remarks else "OK"

    # ----------------- Spain missing-live-games check -----------------
    if expected_spain_fixtures:
        try:
            spain_rows = df_bsr[
                df_bsr["Market"].astype(str).str.lower().str.contains("spain", na=False)
                & df_bsr["TV-Channel"].astype(str).str.lower().str.contains("live", na=False)
            ]
            found = spain_rows.shape[0]
            if found != expected_spain_fixtures:
                note = f"Missing live fixtures (Spain): expected {expected_spain_fixtures}, found {found}"
                logging.warning("⚠️ " + note)
                df_bsr.loc[spain_rows.index, "Market_Channel_Program_Remark"] += "; " + note
        except Exception as e:
            logging.debug(f"Spain fixture check skipped: {e}")

    logging.info("✅ Market & Channel Consistency Check completed.")
    return df_bsr

# -----------------------------------------------------------
def domestic_market_check(df, league_keyword="F24 Spain", debug=False):
    """
    Domestic Market Coverage Check:
    - Focuses on Spain market and LaLiga-related competitions/events.
    - Ensures all Live & Delayed programs are present for each matchday.
    - Highlights & Magazine & Support are marked as Not Applicable.
    """

    logging.info(f" Running domestic market coverage check for league: {league_keyword}")

    # Ensure columns exist
    required_cols = ["Market", "Competition", "Event", "Type of program", "Matchday"]
    for col in required_cols:
        if col not in df.columns:
            logging.warning(f" Missing required column: {col}")
            df["Domestic_Market_Coverage_Check_OK"] = "Not Applicable"
            df["Domestic Market Coverage Remark"] = "Required column missing"
            return df

    # Normalize text
    df["Market"] = df["Market"].astype(str).str.strip()
    df["Competition"] = df["Competition"].astype(str).str.strip()
    df["Event"] = df["Event"].astype(str).str.strip()
    df["Type of program"] = df["Type of program"].astype(str).str.strip()
    df["Matchday"] = df["Matchday"].astype(str).str.strip()

    # Identify Spain markets
    df["is_spain_market"] = df["Market"].str.contains("Spain", case=False, na=False)

    # Identify league-related rows
    league_keywords = [
        "F24 Spain", "LaLiga", "Liga", "Primera", "Segunda", "Liga Española"
    ]
    df["is_laliga"] = df["Competition"].apply(
        lambda x: any(kw.lower() in str(x).lower() for kw in league_keywords)
    ) | df["Event"].apply(
        lambda x: any(kw.lower() in str(x).lower() for kw in league_keywords)
    )

    # Initialize output columns
    df["Domestic_Market_Coverage_Check_OK"] = "Not Applicable"
    df["Domestic Market Coverage Remark"] = "Not Applicable"

    # Get all matchdays for the league
    laliga_rows = df[df["is_laliga"] & df["is_spain_market"]]
    if laliga_rows.empty:
        logging.warning(" No LaLiga (F24 Spain) entries found for Spain market.")
        return df

    all_matchdays = laliga_rows["Matchday"].unique()
    if debug:
        logging.info(f" Found {len(all_matchdays)} matchdays for Spain market: {all_matchdays}")

    # Check for coverage (Live & Delayed) in Spain
    for md in all_matchdays:
        md_rows = laliga_rows[laliga_rows["Matchday"] == md]
        live_present = any(md_rows["Type of program"].str.contains("Live", case=False, na=False))
        delayed_present = any(md_rows["Type of program"].str.contains("Delayed", case=False, na=False))

        # Marking results
        condition = (df["Matchday"] == md) & df["is_laliga"] & df["is_spain_market"]

        if debug:
            logging.info(f"🔍 Matchday {md}: Live={live_present}, Delayed={delayed_present}, Rows={len(md_rows)}")

        if not live_present and not delayed_present:
            df.loc[condition, "Domestic_Market_Coverage_Check_OK"] = "FALSE"
            df.loc[condition, "Domestic Market Coverage Remark"] = f"No live/delayed coverage for matchday {md}"
        elif live_present and delayed_present:
            df.loc[condition, "Domestic_Market_Coverage_Check_OK"] = "TRUE"
            df.loc[condition, "Domestic Market Coverage Remark"] = f"Live & delayed coverage present for matchday {md}"
        elif live_present or delayed_present:
            df.loc[condition, "Domestic_Market_Coverage_Check_OK"] = "TRUE"
            df.loc[condition, "Domestic Market Coverage Remark"] = f"Partial coverage for matchday {md}"

    # For highlights / magazine rows in Spain
    mask_highlights = df["Type of program"].str.contains("Highlight|Magazine", case=False, na=False) & df["is_spain_market"]
    df.loc[mask_highlights, "Domestic_Market_Coverage_Check_OK"] = "Not Applicable"
    df.loc[mask_highlights, "Domestic Market Coverage Remark"] = "Not applicable for highlights or magazine programs"

    # For other markets (non-Spain)
    mask_others = ~df["is_spain_market"]
    df.loc[mask_others, "Domestic_Market_Coverage_Check_OK"] = "Not Applicable"
    df.loc[mask_others, "Domestic Market Coverage Remark"] = "Other market, not applicable"

    if debug:
        true_count = (df["Domestic_Market_Coverage_Check_OK"] == "TRUE").sum()
        false_count = (df["Domestic_Market_Coverage_Check_OK"] == "FALSE").sum()
        na_count = (df["Domestic_Market_Coverage_Check_OK"] == "Not Applicable").sum()
        logging.info(f"✅ Domestic Market Check Summary: TRUE={true_count}, FALSE={false_count}, N/A={na_count}")

    # Drop helper columns
    df.drop(columns=["is_spain_market", "is_laliga"], inplace=True, errors="ignore")
    return df

# -----------------------------------------------------------
# 11️⃣ Rates & Ratings (Audience) Check

def rates_and_ratings_check(df):
    """
    Rates & Ratings QC
    - Uses two columns (detected from headers):
        * Audience Estimates (e.g. "Aud. Estimates ['000s]")
        * Audience Metered  (e.g. "Aud Metered (000s) 3+")
    - Rules:
        - If BOTH columns are empty -> FAIL (Rates_Ratings_QC_OK = False), remark "Both empty"
        - If BOTH columns are present (non-empty, including 0) -> FAIL, remark "Both present"
        - If exactly ONE column is present (including 0) -> PASS, remark "Valid: one rating source present"
    - Returns the same dataframe with two added columns:
        - Rates_Ratings_QC_OK (bool)
        - Rates_Ratings_QC_Remark (str)
    """

    print("\n--- Running Rates & Ratings Check ---")

    # 1) Find the two target columns by header substring (case-insensitive)
    cols = list(df.columns.astype(str))

    def find_col_containing(substrings):
        substrings = [s.lower() for s in substrings]
        for c in cols:
            cl = c.lower()
            if all(sub in cl for sub in substrings):
                return c
        return None

    # likely header tokens based on your samples
    est_col = find_col_containing(["aud", "estim"]) or find_col_containing(["aud. estimates"]) or \
              find_col_containing(["aud. estimates", "000"])  # fallback attempts

    met_col = find_col_containing(["aud", "meter"]) or find_col_containing(["aud metered"]) or \
              find_col_containing(["aud", "metered", "3+"])

    # If not found, fallback to explicit names if present
    if est_col is None and "Aud. Estimates ['000s]" in cols:
        est_col = "Aud. Estimates ['000s]"
    if met_col is None and "Aud Metered (000s) 3+" in cols:
        met_col = "Aud Metered (000s) 3+"

    # Ensure columns exist in df (create empty if missing)
    if est_col is None:
        # choose a safe fallback column name
        est_col = "Aud. Estimates ['000s]"  # consistent with your header sample
        if est_col not in df.columns:
            df[est_col] = pd.NA

    if met_col is None:
        met_col = "Aud Metered (000s) 3+"
        if met_col not in df.columns:
            df[met_col] = pd.NA

    # Helper: decide if a cell is "present" (non-empty) — 0 counts as present
    def is_present(val):
        # NaT/NaN or None
        if pd.isna(val):
            return False
        # strings: strip and check
        if isinstance(val, str):
            s = val.strip()
            if s == "":
                return False
            if s.lower() in {"nan", "none", "na", "n/a"}:
                return False
            # otherwise string (even "0" or "0.00") is present
            return True
        # numbers (including 0) are present
        return True

    # Prepare result columns
    out_ok_col = "Rates_Ratings_QC_OK"
    out_remark_col = "Rates_Ratings_QC_Remark"
    df[out_ok_col] = True
    df[out_remark_col] = ""

    # Evaluate row-wise
    est_series = df[est_col]
    met_series = df[met_col]

    present_est = est_series.apply(is_present)
    present_met = met_series.apply(is_present)

    # Cases:
    both_empty_mask = (~present_est) & (~present_met)
    both_present_mask = (present_est) & (present_met)
    exactly_one_mask = (present_est ^ present_met)  # XOR

    # Assign results
    df.loc[both_empty_mask, out_ok_col] = False
    df.loc[both_empty_mask, out_remark_col] = "Missing audience ratings (both empty)"

    df.loc[both_present_mask, out_ok_col] = False
    df.loc[both_present_mask, out_remark_col] = "Invalid: both metered and estimated present"

    df.loc[exactly_one_mask, out_ok_col] = True
    df.loc[exactly_one_mask, out_remark_col] = "Valid: one rating source available"

    # For any rows not covered above (shouldn't happen), mark Unknown
    other_mask = ~(both_empty_mask | both_present_mask | exactly_one_mask)
    if other_mask.any():
        df.loc[other_mask, out_ok_col] = False
        df.loc[other_mask, out_remark_col] = "Unknown rating status"

    total = len(df)
    failed = (~df[out_ok_col]).sum()
    print(f"Rates & Ratings QC Summary: {failed}/{total} failed ({(failed/total)*100 if total>0 else 0:.2f}%)")
    print(f"Detected estimate column: {est_col} | meter column: {met_col}")

    return df
# -----------------------------------------------------------
# 12️⃣ Comparison of Duplicated Markets

def duplicated_market_check(df, macro_path, league_keyword="F24 Spain", debug=False):
    """
    ✅ Duplicated Markets QC Check 
    -------------------------------------------------
    Goal:
        Identify markets in BSR that are listed as 'Duplicate Markets'
        in the Macro file (Data Core sheet) for a given league.

    Inputs:
        df: BSR DataFrame
        macro_path: path to Macro Market Duplicator file (.xlsm)
        league_keyword: league name (default = "F24 Spain")
        debug: if True, prints debug information

    Output:
        Adds:
            - Duplicated_Markets_Check_OK  (TRUE / FALSE / Not Applicable / Error)
            - Duplicated_Markets_Remark
    """

    result_col = "Duplicated_Markets_Check_OK"
    remark_col = "Duplicated_Markets_Remark"

    # Case 1️⃣: No Macro file
    if not macro_path or not os.path.exists(macro_path):
        df[result_col] = "Not Applicable"
        df[remark_col] = "Macro file missing"
        return df

    try:
        # --- Load and clean Macro Data ---
        macro_df = pd.read_excel(macro_path, sheet_name="Data Core", header=1)
        macro_df.columns = macro_df.columns.str.strip().str.replace('\xa0', ' ', regex=True)

        required_cols = ["Projects", "Dup Market"]
        if not all(c in macro_df.columns for c in required_cols):
            missing = [c for c in required_cols if c not in macro_df.columns]
            raise ValueError(f"Missing columns in Macro file: {missing}")

        if debug:
            print("✅ Macro columns:", macro_df.columns.tolist())

        # --- Filter Macro by League Keyword ---
        macro_filtered = macro_df[
            macro_df["Projects"].astype(str).str.contains(league_keyword, case=False, na=False)
        ].copy()

        if macro_filtered.empty:
            df[result_col] = "Not Applicable"
            df[remark_col] = f"No matching league ({league_keyword}) found in Macro"
            return df

        # --- Extract Duplicate Market List ---
        duplicate_markets = (
            macro_filtered["Dup Market"].dropna().astype(str).str.strip().str.lower().unique().tolist()
        )

        if debug:
            print(f"🔍 Duplicate Markets for {league_keyword}: {duplicate_markets}")

        # --- Normalize BSR Data ---
        df["Market_clean"] = df["Market"].astype(str).str.strip().str.lower()
        df["Competition_clean"] = df["Competition"].astype(str).str.strip().str.lower()
        df["Event_clean"] = df["Event"].astype(str).str.strip().str.lower()

        # --- Determine if each row belongs to the selected league ---
        in_league_mask = (
            df["Competition_clean"].str.contains(league_keyword.lower(), na=False)
            | df["Event_clean"].str.contains(league_keyword.lower(), na=False)
        )

        # --- Initialize result columns ---
        df[result_col] = "Not Applicable"
        df[remark_col] = "Different competition/event"

        # --- Apply vectorized duplicate check ---
        league_df = df[in_league_mask]
        df.loc[in_league_mask, result_col] = league_df["Market_clean"].isin(duplicate_markets).map(
            {True: "FALSE", False: "TRUE"}
        )

        df.loc[in_league_mask, remark_col] = league_df["Market_clean"].apply(
            lambda m: f"Duplicate market ({m}) found in Macro"
            if m in duplicate_markets
            else f"Valid market ({m}) not listed as duplicate"
        )

        # --- Cleanup temporary columns ---
        df.drop(columns=["Market_clean", "Competition_clean", "Event_clean"], inplace=True, errors="ignore")

        if debug:
            print("✅ Duplicated Market Check completed successfully.")

        return df

    except Exception as e:
        print(f"❌ Error in duplicated_market_check: {e}")
        df[result_col] = "Error"
        df[remark_col] = str(e)
        return df
# -----------------------------------------------------------
# 13️⃣ Country & Channel IDs Check
def country_channel_id_check(df):
    """
    Ensures that each channel and market is mapped to a single, consistent ID.
    Outputs two columns:
      - Market_Channel_ID_OK (True/False)
      - Market_Channel_ID_Remark (string)
    """

    df_result = df.copy()
    df_result["Market_Channel_ID_OK"] = True
    df_result["Market_Channel_ID_Remark"] = ""

    def norm(x):
        return str(x).strip() if pd.notna(x) else ""

    # Maps to track consistency
    channel_id_map = {}
    market_id_map = {}

    for idx, row in df_result.iterrows():
        channel = norm(row.get("TV-Channel"))
        channel_id = norm(row.get("Channel ID"))
        market = norm(row.get("Market"))
        market_id = norm(row.get("Market ID"))

        remarks = []
        ok = True

        # ✅ Check 1 – Same channel shouldn't have multiple Channel IDs
        if channel:
            if channel in channel_id_map and channel_id_map[channel] != channel_id:
                remarks.append(
                    f"Channel '{channel}' has multiple IDs ({channel_id_map[channel]} vs {channel_id})"
                )
                ok = False
            else:
                channel_id_map[channel] = channel_id

        # ✅ Check 2 – Same market shouldn't have multiple Market IDs
        if market:
            if market in market_id_map and market_id_map[market] != market_id:
                remarks.append(
                    f"Market '{market}' has multiple IDs ({market_id_map[market]} vs {market_id})"
                )
                ok = False
            else:
                market_id_map[market] = market_id

        # ✅ Check 3 – Same Channel ID shouldn't be used for multiple channels
        if channel_id and list(channel_id_map.values()).count(channel_id) > 1:
            remarks.append(f"Channel ID '{channel_id}' assigned to multiple channels")
            ok = False

        # ✅ Check 4 – Same Market ID shouldn't be used for multiple markets
        if market_id and list(market_id_map.values()).count(market_id) > 1:
            remarks.append(f"Market ID '{market_id}' assigned to multiple markets")
            ok = False

        # ✅ Write results
        df_result.at[idx, "Market_Channel_ID_OK"] = ok
        df_result.at[idx, "Market_Channel_ID_Remark"] = "; ".join(remarks) if remarks else "OK"

    return df_result

# -----------------------------------------------------------
def client_lstv_ott_check(df_worksheet, project_config=None):
    """
    Ensures:
      - Each Channel ID maps consistently to one Market ID
      - Each Market ID maps consistently to one Channel ID
      - Client/LSTV/OTT presence is checked
    """
    df = df_worksheet.copy()
    df["Client_LSTV_OTT_OK"] = True
    df["Client_LSTV_OTT_Remark"] = ""

    # --- Normalize values ---
    def norm(x):
        return str(x).strip().lower() if pd.notna(x) else ""

    # --- Build mapping dicts ---
    channel_to_market = {}
    market_to_channel = {}

    for idx, row in df.iterrows():
        ch_id = norm(row.get("Channel ID"))
        mk_id = norm(row.get("Market ID"))

        remarks = []
        ok = True

        # 🔹 Channel → Market mapping consistency
        if ch_id:
            if ch_id in channel_to_market and channel_to_market[ch_id] != mk_id:
                remarks.append(f"Channel ID {ch_id} linked to multiple Market IDs ({channel_to_market[ch_id]} vs {mk_id})")
                ok = False
            else:
                channel_to_market[ch_id] = mk_id

        # 🔹 Market → Channel mapping consistency
        if mk_id:
            if mk_id in market_to_channel and market_to_channel[mk_id] != ch_id:
                remarks.append(f"Market ID {mk_id} linked to multiple Channel IDs ({market_to_channel[mk_id]} vs {ch_id})")
                ok = False
            else:
                market_to_channel[mk_id] = ch_id

        # 🔹 Client / LSTV / OTT check
        pay_col = "Pay/Free TV"
        if pay_col in df.columns:
            val = norm(row.get(pay_col, ""))
            if not any(k in val for k in ["client", "lstv", "ott"]):
                remarks.append(f"Missing Client/LSTV/OTT source: {row.get(pay_col, '')}")
                ok = False

        df.at[idx, "Client_LSTV_OTT_OK"] = ok
        df.at[idx, "Client_LSTV_OTT_Remark"] = "; ".join(remarks) if remarks else "OK"

    return df
# -----------------------------------------------------------
# ✅ Excel Coloring for True/False checks
def color_excel(output_path, df):
    from openpyxl import load_workbook
    from openpyxl.styles import PatternFill

    GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")

    wb = load_workbook(output_path)
    ws = wb.active
    headers = [cell.value for cell in ws[1]]
    col_map = {name: idx+1 for idx, name in enumerate(headers)}

    qc_columns = [col for col in df.columns if col.endswith("_OK")]

    for col_name in qc_columns:
        if col_name in col_map:
            col_idx = col_map[col_name]
            for row in range(2, ws.max_row + 1):
                cell = ws.cell(row=row, column=col_idx)
                val = cell.value
                if val in [True, "True"]:
                    cell.fill = GREEN_FILL
                elif val in [False, "False"]:
                    cell.fill = RED_FILL

    wb.save(output_path)
# -----------------------------------------------------------
# Summary Sheet
def generate_summary_sheet(output_path, df):
    wb = load_workbook(output_path)
    if "Summary" in wb.sheetnames: del wb["Summary"]
    ws = wb.create_sheet("Summary")

    qc_columns = [col for col in df.columns if "_OK" in col]
    summary_data = []
    for col in qc_columns:
        total = len(df)
        passed = df[col].sum() if df[col].dtype==bool else sum(df[col]=="True")
        summary_data.append([col, total, passed, total - passed])

    summary_df = pd.DataFrame(summary_data, columns=["Check", "Total", "Passed", "Failed"])
    for r in dataframe_to_rows(summary_df, index=False, header=True):
        ws.append(r)
    wb.save(output_path)
