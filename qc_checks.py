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
    Completeness check (robust).
    - Mandatory: TV Channel, Channel ID, Match Day, Source (must be non-empty)
    - Audience: either Aud. Estimates OR Aud Metered must be present (0 counts as present)
    - If Type of Program is live/repeat/delayed -> Home Team and Away Team must be present
    - Adds two columns: Completeness_OK (bool) and Completeness_Remark (string)
    """
    # --- Candidate header names (case-insensitive match)
    required_columns = {
        "tv_channel": ["TV Channel", "TV-Channel", "Channel", "TV Channel "],
        "channel_id": ["Channel ID", "ChannelID", "Channel Id"],
        "type_of_program": ["Type of Program", "Type of programme", "Type of program"],
        "match_day": ["Matchday", "Match Day", "Matchday "],
        "home_team": ["Home Team", "HomeTeam", "Home"],
        "away_team": ["Away Team", "AwayTeam", "Away"],
        "aud_estimates": ["Aud. Estimates ['000s]", "Audience Estimates", "Aud Estimates"],
        "aud_metered": ["Aud Metered (000s) 3+", "Audience Metered", "Aud. Metered (000s) 3+"],
        # ✅ Expanded Source detection to include Audience Source variations
        "source": ["Source", "AudienceSource", "Audience Source", "Audience_Source"]
    }

    # Build a mapping from logical keys to actual dataframe column names (case-insensitive)
    lower_to_actual = {col.lower(): col for col in df.columns}
    colmap = {}
    for key, opts in required_columns.items():
        found = None
        for opt in opts:
            for col_lower, actual_col in lower_to_actual.items():
                if opt.lower() == col_lower:
                    found = actual_col
                    break
            if found:
                break
        colmap[key] = found  # may be None if not found

    # ✅ Special case: if "Audience Source" exists but "Source" doesn't, use it as Source
    if not colmap.get("source"):
        for col in df.columns:
            if "audience source" in col.lower():
                colmap["source"] = col
                break

    # Helper to decide if a cell is "present"
    def is_present(val):
        if val is None:
            return False
        try:
            if pd.isna(val):
                return False
        except Exception:
            pass
        if isinstance(val, (int, float)) and not (isinstance(val, float) and pd.isna(val)):
            return True
        s = str(val).replace("\xa0", "").strip()
        if s == "" or s.lower() in ["nan", "none"]:
            return False
        return True

    # Prepare result columns
    df["Completeness_OK"] = True
    df["Completeness_Remark"] = ""

    live_types = {"live", "repeat", "delayed"}
    relaxed_types = {"highlights", "magazine", "support", "magazine and support"}

    # Iterate rows
    for idx, row in df.iterrows():
        missing = []

        # 1) Mandatory columns: TV Channel, Channel ID, Match Day, Source
        for logical, display in [
            ("tv_channel", "TV Channel"),
            ("channel_id", "Channel ID"),
            ("match_day", "Match Day"),
            ("source", "Source")
        ]:
            colname = colmap.get(logical)
            if colname is None:
                missing.append(f"{display} (column not found)")
            else:
                if not is_present(row.get(colname)):
                    missing.append(display)

        # 2) Audience: either aud_estimates OR aud_metered must be present
        aud_est_col = colmap.get("aud_estimates")
        aud_met_col = colmap.get("aud_metered")
        aud_est_present = False
        aud_met_present = False
        if aud_est_col is not None:
            aud_est_present = is_present(row.get(aud_est_col))
        if aud_met_col is not None:
            aud_met_present = is_present(row.get(aud_met_col))

        if (aud_est_col is None) and (aud_met_col is None):
            missing.append("Audience (Estimates/Metered) (columns not found)")
        else:
            if not (aud_est_present or aud_met_present):
                missing.append("Audience (Estimates/Metered)")

        # 3) Home/Away requirement depending on Type of Program
        type_col = colmap.get("type_of_program")
        prog_type = str(row.get(type_col) or "").strip().lower() if type_col else ""

        home_col = colmap.get("home_team")
        away_col = colmap.get("away_team")

        if prog_type in live_types:
            if home_col is None:
                missing.append("Home Team (column not found)")
            elif not is_present(row.get(home_col)):
                missing.append("Home Team")

            if away_col is None:
                missing.append("Away Team (column not found)")
            elif not is_present(row.get(away_col)):
                missing.append("Away Team")

        elif prog_type in relaxed_types:
            pass
        else:
            if home_col is not None and not is_present(row.get(home_col)):
                missing.append("Home Team")
            if away_col is not None and not is_present(row.get(away_col)):
                missing.append("Away Team")

        # Finalize per-row result
        if missing:
            df.at[idx, "Completeness_OK"] = False
            df.at[idx, "Completeness_Remark"] = "; ".join(missing)
        else:
            df.at[idx, "Completeness_OK"] = True
            df.at[idx, "Completeness_Remark"] = "All key fields present"

    return df

# ----------------------------- 5️⃣ Overlap / Duplicate / Day Break -----------------------------
def overlap_duplicate_daybreak_check(df):
    """
    Non-destructive Overlap / Duplicate / Daybreak QC.
    Returns a new DataFrame with original columns intact + these added columns:
      - Overlap_OK (bool), Overlap_Remark (str)
      - Duplicate_OK (bool), Duplicate_Remark (str)
      - Daybreak_OK (bool), Daybreak_Remark (str)
    """
    if df is None:
        return df

    # work on deep copy for safety
    df_in = df.copy(deep=True)

    # ---------- auto-detect column names (case-insensitive) ----------
    def find_col(sub):
        return next((c for c in df_in.columns if sub.lower() in str(c).lower()), None)

    col_channel       = find_col("channel") or "TV Channel"
    col_channel_id    = find_col("channel id") or "Channel ID"
    col_date_utc      = find_col("date (utc") or find_col("date") or "Date (UTC/GMT)"
    col_start_utc     = find_col("start (utc") or find_col("start") or "Start (UTC)"
    col_end_utc       = find_col("end (utc") or find_col("end") or "End (UTC)"
    col_payfree       = find_col("pay") or "Pay/Free"
    col_combined      = find_col("combined") or "Combined"

    # ---------- create a working sorted view (keep original index) ----------
    # parse times into temporary columns (do NOT overwrite original cols)
    tmp_start = pd.to_datetime(df_in[col_start_utc].astype(str).str.strip(), format="%H:%M:%S", errors="coerce") \
                if col_start_utc in df_in.columns else pd.NaT
    tmp_end   = pd.to_datetime(df_in[col_end_utc].astype(str).str.strip(),   format="%H:%M:%S", errors="coerce") \
                if col_end_utc in df_in.columns else pd.NaT

    work = df_in.assign(_qc_start_dt=tmp_start, _qc_end_dt=tmp_end, _orig_index=df_in.index)

    sort_by = [c for c in (col_channel, col_date_utc, "_qc_start_dt") if c in work.columns]
    if sort_by:
        work = work.sort_values(by=sort_by, na_position="last").reset_index(drop=True)
    else:
        work = work.reset_index(drop=True)

    # ---------- OVERLAP CHECK ----------
    overlap_ok = pd.Series(True, index=work.index)
    overlap_remark = pd.Series("", index=work.index)

    try:
        # create OTT flag (do not assign back to original df)
        is_ott = pd.Series(False, index=work.index)
        if col_payfree in work.columns:
            is_ott = work[col_payfree].fillna("").astype(str).str.lower().str.contains("ott|internet|www")

        prev_end = work["_qc_end_dt"].shift(1)
        same_channel = work[col_channel] == work[col_channel].shift(1) if col_channel in work.columns else pd.Series(False, index=work.index)
        same_date = work[col_date_utc] == work[col_date_utc].shift(1) if col_date_utc in work.columns else pd.Series(False, index=work.index)

        # overlap when same channel+date, not OTT, and start < previous end (both datetimes present)
        overlap_mask = same_channel & same_date & (~is_ott) & work["_qc_start_dt"].notna() & prev_end.notna() & (work["_qc_start_dt"] < prev_end)

        overlap_ok.loc[overlap_mask] = False
        overlap_remark.loc[overlap_mask] = "Overlap detected between consecutive events"
    except Exception as e:
        # fail-safe: mark all True and leave remark empty
        print(f"⚠️ Overlap logic failed: {e}")

    # ---------- DUPLICATE CHECK ----------
    duplicate_ok = pd.Series(True, index=work.index)
    duplicate_remark = pd.Series("", index=work.index)

    try:
        # duplicate columns chosen from original names available (must include channel + date + start + end)
        dup_cols = [c for c in (col_channel, col_date_utc, col_start_utc, col_end_utc) if c in df_in.columns]
        if dup_cols:
            # Use df_in (original index alignment) for duplicated to avoid affecting work-sort ordering:
            dup_mask_full = df_in.duplicated(subset=dup_cols, keep=False)
            # map dup_mask_full (original index) to work's rows via orig_index
            dup_mask_work = work["_orig_index"].map(lambda i: dup_mask_full.iloc[i] if i in dup_mask_full.index else False)
            duplicate_ok.loc[dup_mask_work] = False
            duplicate_remark.loc[dup_mask_work] = "Duplicate row found"
    except Exception as e:
        print(f"⚠️ Duplicate logic failed: {e}")

    # ---------- DAYBREAK CHECK ----------
    daybreak_ok = pd.Series(True, index=work.index)
    daybreak_remark = pd.Series("", index=work.index)

    try:
        # iterate adjacent rows in work (sorted) — safe and small overhead
        for i in range(1, len(work)):
            curr = work.iloc[i]
            prev = work.iloc[i - 1]

            same_channel_val = (col_channel in work.columns and curr.get(col_channel) == prev.get(col_channel))
            same_channel_id  = (col_channel_id in work.columns and curr.get(col_channel_id) == prev.get(col_channel_id))
            same_combined    = (col_combined in work.columns and curr.get(col_combined) == prev.get(col_combined))

            if same_channel_val and same_channel_id and same_combined:
                # continuation candidate: ensure both end & start dt are present
                if pd.notna(prev["_qc_end_dt"]) and pd.notna(curr["_qc_start_dt"]):
                    gap_min = (curr["_qc_start_dt"] - prev["_qc_end_dt"]).total_seconds() / 60.0
                    # allow small gaps representing minor program split (0–2 min)
                    if gap_min < 0 or gap_min > 2:
                        daybreak_ok.iat[i] = False
                        daybreak_remark.iat[i] = "Time gap too large for continuation"
            else:
                # if start day differs from previous end day -> suspicious daybreak
                if pd.notna(prev["_qc_end_dt"]) and pd.notna(curr["_qc_start_dt"]):
                    if curr["_qc_start_dt"].day != prev["_qc_end_dt"].day:
                        daybreak_ok.iat[i] = False
                        daybreak_remark.iat[i] = "Different program continuation across days"
    except Exception as e:
        print(f"⚠️ Daybreak logic failed: {e}")

    # ---------- Map results back to original DataFrame order ----------
    # indexes in 'work' correspond to rows; map them to original index via _orig_index
    res_df = pd.DataFrame({
        "Overlap_OK": overlap_ok,
        "Overlap_Remark": overlap_remark,
        "Duplicate_OK": duplicate_ok,
        "Duplicate_Remark": duplicate_remark,
        "Daybreak_OK": daybreak_ok,
        "Daybreak_Remark": daybreak_remark,
    }, index=work.index)

    # create output as original dataframe copy and append results aligned by original index
    out = df_in.copy(deep=True)
    # ensure alignment by original index
    res_df_with_orig_idx = res_df.copy()
    res_df_with_orig_idx["_orig_index"] = work["_orig_index"].values
    # set index to orig index and then reindex to out
    res_df_with_orig_idx = res_df_with_orig_idx.set_index("_orig_index").reindex(out.index)

    # assign columns (if already exist, they'll be overwritten with result values)
    for col in ["Overlap_OK", "Overlap_Remark", "Duplicate_OK", "Duplicate_Remark", "Daybreak_OK", "Daybreak_Remark"]:
        out[col] = res_df_with_orig_idx[col].values

    return out


# ----------------------------- 6️⃣ Program Category Check -----------------------------
def program_category_check(filepath, df_bsr):
    """
    Program Category Check:
    Compares BSR data with Fixture List to validate program type (Live/Repeat/Highlights).
    """

    import pandas as pd
    import logging

    logging.info("🔍 Starting Program Category Check...")

    # --- Load Fixture List Sheet ---
    xl = pd.ExcelFile(filepath)
    fixture_sheet = next((s for s in xl.sheet_names if "fixture" in s.lower()), None)
    if not fixture_sheet:
        logging.warning("⚠️ Fixture list sheet not found.")
        df_bsr["Program_Category_Expected"] = "unknown"
        df_bsr["Program_Category_Actual"] = df_bsr.get("Type of Program", "")
        df_bsr["Program_Category_OK"] = False
        df_bsr["Program_Category_Remark"] = "Fixture list sheet missing"
        return df_bsr

    df_fix = xl.parse(fixture_sheet)
    logging.info(f"✅ Loaded Fixture sheet '{fixture_sheet}' ({len(df_fix)} rows)")

    # --- Identify Columns ---
    def find_col(df, keywords):
        for col in df.columns:
            if any(k in col.lower() for k in keywords):
                return col
        return None

    col_bsr_progtype = find_col(df_bsr, ["type"])
    col_bsr_start = find_col(df_bsr, ["start"])
    col_bsr_end = find_col(df_bsr, ["end"])
    col_bsr_home = find_col(df_bsr, ["home"])
    col_bsr_away = find_col(df_bsr, ["away"])

    col_fix_progtype = find_col(df_fix, ["type"])
    col_fix_start = find_col(df_fix, ["start"])
    col_fix_end = find_col(df_fix, ["end"])
    col_fix_home = find_col(df_fix, ["home"])
    col_fix_away = find_col(df_fix, ["away"])

    # --- Convert time columns to datetime ---
    for c in [col_bsr_start, col_bsr_end, col_fix_start, col_fix_end]:
        if c is not None:
            try:
                if c in df_bsr.columns:
                    df_bsr[c] = pd.to_datetime(df_bsr[c], errors="coerce")
                elif c in df_fix.columns:
                    df_fix[c] = pd.to_datetime(df_fix[c], errors="coerce")
            except Exception:
                pass

    expected_list, actual_list, ok_list, remark_list = [], [], [], []

    for i, bsr_row in df_bsr.iterrows():
        actual = str(bsr_row.get(col_bsr_progtype, "")).strip().lower()
        home_team = str(bsr_row.get(col_bsr_home, "")).strip().lower()
        away_team = str(bsr_row.get(col_bsr_away, "")).strip().lower()
        bsr_start = bsr_row.get(col_bsr_start)
        bsr_end = bsr_row.get(col_bsr_end)

        expected = "unknown"
        ok = True
        remark = "OK"

        # --- Match fixture row ---
        fix_match = df_fix[
            (df_fix[col_fix_home].astype(str).str.lower() == home_team)
            | (df_fix[col_fix_away].astype(str).str.lower() == away_team)
        ]

        if fix_match.empty:
            expected = "unknown"
            ok = False
            remark = "No matching fixture found"
        else:
            fix_row = fix_match.iloc[0]
            fix_start = fix_row.get(col_fix_start)
            fix_end = fix_row.get(col_fix_end)

            if pd.isna(fix_start) or pd.isna(fix_end) or pd.isna(bsr_start) or pd.isna(bsr_end):
                expected = "unknown"
                ok = False
                remark = "Invalid time values"
            else:
                start_diff = abs((bsr_start - fix_start).total_seconds()) / 60
                end_diff = abs((bsr_end - fix_end).total_seconds()) / 60

                # --- Determine expected category ---
                if start_diff <= 30 and end_diff <= 30:
                    expected = "live"
                elif start_diff <= 70 or end_diff <= 70:
                    expected = "repeat"
                else:
                    expected = "highlights"

                # --- Compare ---
                if expected == actual:
                    ok = True
                    remark = "OK"
                else:
                    ok = False
                    remark = f"Expected '{expected}', found '{actual}'"

        expected_list.append(expected)
        actual_list.append(actual)
        ok_list.append(ok)
        remark_list.append(remark)

        # Console debug print
        print(f"[{i}] Home={home_team} Away={away_team} Expected={expected} Actual={actual} OK={ok} Remark={remark}")

    df_bsr["Program_Category_Expected"] = expected_list
    df_bsr["Program_Category_Actual"] = actual_list
    df_bsr["Program_Category_OK"] = ok_list
    df_bsr["Program_Category_Remark"] = remark_list

    logging.info("✅ Program Category Check completed successfully.")
    return df_bsr


# ----------------------------- 7️⃣ Duration Check -----------------------------
def duration_check(df):
    """
    Duration Check:
    1️⃣ Combines Date (UTC/GMT) + Start(UTC)/End(UTC)
    2️⃣ Calculates actual duration in minutes
    3️⃣ Validates by program type (Live/Repeat/Highlights/Magazine)
    4️⃣ Enforces BSA ≤ 180 mins rule for Live/Repeat
    """

    import pandas as pd
    import logging

    logging.info("🚀 Starting Duration Check...")

    # --- Identify relevant columns ---
    col_date = next((c for c in df.columns if "date" in c.lower() and "gmt" in c.lower()), None)
    col_start = next((c for c in df.columns if "start" in c.lower() and "utc" in c.lower()), None)
    col_end = next((c for c in df.columns if "end" in c.lower() and "utc" in c.lower()), None)
    col_type = next((c for c in df.columns if "type" in c.lower()), None)
    col_desc = next((c for c in df.columns if "desc" in c.lower()), None)
    col_source = next((c for c in df.columns if "source" in c.lower()), None)

    logging.info(f"✅ Using columns: Date={col_date}, Start={col_start}, End={col_end}, Type={col_type}, Source={col_source}")

    df["Duration_OK"], df["Duration_Remark"], df["Duration_Mins"] = False, "", None

    # --- Combine date + time for UTC columns ---
    try:
        df["_StartDT"] = pd.to_datetime(df[col_date].astype(str) + " " + df[col_start].astype(str), errors="coerce", dayfirst=True, utc=True)
        df["_EndDT"] = pd.to_datetime(df[col_date].astype(str) + " " + df[col_end].astype(str), errors="coerce", dayfirst=True, utc=True)
    except Exception as e:
        logging.error(f"⚠️ Error combining datetime columns: {e}")

    # --- Compute duration ---
    df["Duration_Mins"] = (df["_EndDT"] - df["_StartDT"]).dt.total_seconds() / 60

    for i, row in df.iterrows():
        start, end, dur = row["_StartDT"], row["_EndDT"], row["Duration_Mins"]
        ptype = str(row.get(col_type, "")).lower()
        desc = str(row.get(col_desc, "")).lower()
        source = str(row.get(col_source, "")).lower()

        # Validate time
        if pd.isna(start) or pd.isna(end) or pd.isna(dur):
            df.at[i, "Duration_Remark"] = "Invalid or missing Start/End UTC"
            continue

        # --- Classify expected type based on duration ---
        expected = None
        if dur >= 120: expected = "live"
        elif 60 <= dur < 120: expected = "repeat"
        elif 30 <= dur < 60: expected = "highlights"
        else: expected = "magazine/support"

        # --- Special rule for Football (BSA) ---
        if "bsa" in source and (ptype in ["live", "repeat"]) and dur > 180:
            df.at[i, "Duration_OK"] = False
            df.at[i, "Duration_Remark"] = "BSA Live/Repeat > 180 mins (Invalid)"
            continue

        # --- Compare actual vs expected ---
        if expected in ptype or expected in desc:
            df.at[i, "Duration_OK"] = True
            df.at[i, "Duration_Remark"] = "OK"
        else:
            df.at[i, "Duration_OK"] = False
            df.at[i, "Duration_Remark"] = f"Expected {expected}, found {ptype or desc}"

    logging.info("✅ Duration Check Completed.")
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

    logging.info(f"🏠 Running domestic market coverage check for league: {league_keyword}")

    # Ensure columns exist
    required_cols = ["Market", "Competition", "Event", "Type of program", "Matchday"]
    for col in required_cols:
        if col not in df.columns:
            logging.warning(f"⚠️ Missing required column: {col}")
            df["Domestic Market Coverage Check_OK"] = "Not Applicable"
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
    df["Domestic Market Coverage Check_OK"] = "Not Applicable"
    df["Domestic Market Coverage Remark"] = "Not Applicable"

    # Get all matchdays for the league
    laliga_rows = df[df["is_laliga"] & df["is_spain_market"]]
    if laliga_rows.empty:
        logging.warning("⚠️ No LaLiga (F24 Spain) entries found for Spain market.")
        return df

    all_matchdays = laliga_rows["Matchday"].unique()
    if debug:
        logging.info(f"🧩 Found {len(all_matchdays)} matchdays for Spain market: {all_matchdays}")

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
            df.loc[condition, "Domestic Market Coverage Check_OK"] = "FALSE"
            df.loc[condition, "Domestic Market Coverage Remark"] = f"No live/delayed coverage for matchday {md}"
        elif live_present and delayed_present:
            df.loc[condition, "Domestic Market Coverage Check_OK"] = "TRUE"
            df.loc[condition, "Domestic Market Coverage Remark"] = f"Live & delayed coverage present for matchday {md}"
        elif live_present or delayed_present:
            df.loc[condition, "Domestic Market Coverage Check_OK"] = "TRUE"
            df.loc[condition, "Domestic Market Coverage Remark"] = f"Partial coverage for matchday {md}"

    # For highlights / magazine rows in Spain
    mask_highlights = df["Type of program"].str.contains("Highlight|Magazine", case=False, na=False) & df["is_spain_market"]
    df.loc[mask_highlights, "Domestic Market Coverage Check_OK"] = "Not Applicable"
    df.loc[mask_highlights, "Domestic Market Coverage Remark"] = "Not applicable for highlights or magazine programs"

    # For other markets (non-Spain)
    mask_others = ~df["is_spain_market"]
    df.loc[mask_others, "Domestic Market Coverage Check_OK"] = "Not Applicable"
    df.loc[mask_others, "Domestic Market Coverage Remark"] = "Other market, not applicable"

    if debug:
        true_count = (df["Domestic Market Coverage Check_OK"] == "TRUE").sum()
        false_count = (df["Domestic Market Coverage Check_OK"] == "FALSE").sum()
        na_count = (df["Domestic Market Coverage Check_OK"] == "Not Applicable").sum()
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
    🧩 Duplicated Markets QC Check
    --------------------------------
    Goal:
        Identify if any markets in the BSR file are marked as 'Duplicate Markets'
        in the Macro's "Data Core" sheet for the selected league (e.g. 'F24 Spain').

    Inputs:
        - df: BSR DataFrame.
        - macro_path: path to the Macro Market Duplicator file (.xlsm).
        - league_keyword: the league name (e.g., 'F24 Spain').
        - debug: if True, prints additional debug logs.

    Output:
        Adds two columns to df:
            - Duplicated Markets Check_OK  → TRUE / FALSE / Not Applicable
            - Duplicated Markets Remark    → reason for the result
    """

    result_col = "Duplicated Markets Check_OK"
    remark_col = "Duplicated Markets Remark"

    # --- Case 1: Missing macro file ---
    if not macro_path or not os.path.exists(macro_path):
        df[result_col] = "Not Applicable"
        df[remark_col] = "Macro file missing"
        return df

    try:
        # 🧠 Read 'Data Core' sheet (headers start on 2nd row)
        macro_df = pd.read_excel(macro_path, sheet_name="Data Core", header=1)
        macro_df.columns = macro_df.columns.str.strip().str.replace('\xa0', ' ', regex=True)

        required_cols = ["Projects", "Dup Market"]
        missing_cols = [c for c in required_cols if c not in macro_df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns in Macro file: {missing_cols}")

        if debug:
            print("✅ Macro columns:", macro_df.columns.tolist())

        # --- Filter rows for selected league ---
        macro_filtered = macro_df[
            macro_df["Projects"].astype(str).str.contains(league_keyword, case=False, na=False)
        ].copy()

        if macro_filtered.empty:
            df[result_col] = "Not Applicable"
            df[remark_col] = f"No matching league ({league_keyword}) found in Macro"
            return df

        # --- Get list of duplicate markets ---
        duplicate_markets = macro_filtered["Dup Market"].dropna().unique().tolist()
        duplicate_markets_clean = [m.strip().lower() for m in duplicate_markets]

        if debug:
            print(f"🔍 Duplicate markets for {league_keyword}: {duplicate_markets_clean}")

        # --- Normalize data for matching ---
        df["Market_clean"] = df["Market"].astype(str).str.strip().str.lower()
        df["Competition_clean"] = df["Competition"].astype(str).str.strip().str.lower()
        df["Event_clean"] = df["Event"].astype(str).str.strip().str.lower()

        # --- Initialize columns ---
        df[result_col] = "Not Applicable"
        df[remark_col] = "Not applicable"

        # --- Apply logic row by row ---
        for i, row in df.iterrows():
            market = row["Market_clean"]
            comp = row["Competition_clean"]
            event = row["Event_clean"]

            # Only consider rows for this league
            if league_keyword.lower() not in comp and league_keyword.lower() not in event:
                df.at[i, result_col] = "Not Applicable"
                df.at[i, remark_col] = "Different competition/event"
                continue

            if market in duplicate_markets_clean:
                df.at[i, result_col] = "FALSE"
                df.at[i, remark_col] = f"Duplicate market ({market}) found in Macro"
            else:
                df.at[i, result_col] = "TRUE"
                df.at[i, remark_col] = f"Valid market ({market}) not listed as duplicate"

        # --- Cleanup ---
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
# 14️⃣ Client Data / LSTV / OTT Check (corrected)
def client_lstv_ott_check(df_worksheet, project_config=None):
    """
    Checks:
      - Market and Channel ID consistency
      - Inclusion of Client Data, LSTV, OTT sources
    Returns:
      df with:
        - Client_LSTV_OTT_OK (True/False)
        - Client_LSTV_OTT_Remark
    """

    df = df_worksheet.copy()
    df["Client_LSTV_OTT_OK"] = True
    df["Client_LSTV_OTT_Remark"] = ""

    # --- 1️⃣ Market / Channel ID consistency ---
    if "Market ID" in df.columns and "Channel ID" in df.columns:
        # Identify Channel IDs belonging to multiple Market IDs
        multi_market = df.groupby("Channel ID")["Market ID"].nunique()
        multi_market_channels = multi_market[multi_market > 1].index.tolist()

        # Identify Market IDs belonging to multiple Channel IDs
        multi_channel = df.groupby("Market ID")["Channel ID"].nunique()
        multi_channel_ids = multi_channel[multi_channel > 1].index.tolist()
    else:
        multi_market_channels = []
        multi_channel_ids = []

    # --- 2️⃣ Client / LSTV / OTT inclusion ---
    pay_free_col = "Pay/Free TV" if "Pay/Free TV" in df.columns else None

    # Define expected sources
    expected_sources = ["lstv", "client", "ott"]

    for idx, row in df.iterrows():
        remarks = []
        ok = True

        # Market / Channel mapping issues
        if row.get("Channel ID") in multi_market_channels:
            ok = False
            remarks.append("Channel assigned to multiple Market IDs")

        if row.get("Market ID") in multi_channel_ids:
            ok = False
            remarks.append("Market ID assigned to multiple Channel IDs")

        # Client / LSTV / OTT source checks
        if pay_free_col:
            val = str(row.get(pay_free_col, "")).strip().lower()
            # Only mark False if none of the expected sources are present
            if not any(source in val for source in expected_sources):
                ok = False
                remarks.append(f"Missing required source (Client/LSTV/OTT): {row.get(pay_free_col, '')}")

        # Write results
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