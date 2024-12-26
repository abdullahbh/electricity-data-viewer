import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import pytz
import sys

def next_quarter_hour(now):
    quarter = now.minute // 15
    if now.minute % 15 == 0 and now.second == 0:
        next_quarter = quarter
    else:
        next_quarter = quarter + 1

    new_minute = next_quarter * 15
    new_hour = now.hour
    new_day = now.day

    if new_minute == 60:
        new_minute = 0
        new_hour += 1
        if new_hour == 24:
            new_hour = 0
            new_day += 1

    return now.replace(
        hour=new_hour,
        minute=new_minute,
        second=0,
        microsecond=0,
        day=new_day
    )

def fetch_and_process_data():
    try:
        url = "https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/vnitrodenni-trh"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        container = soup.find("p", class_="report_attachment_links")
        if not container:
            raise ValueError("Failed to find the report attachment container.")

        link_tag = container.find("a")
        if not link_tag or not link_tag.get("href"):
            raise ValueError("Failed to find the download link.")

        file_href = link_tag["href"]
        file_link = "https://www.ote-cr.cz" + file_href
        file_response = requests.get(file_link, timeout=10)
        file_response.raise_for_status()

        # Read the Excel file
        excel_file = BytesIO(file_response.content)
        df = pd.read_excel(excel_file, header=None)
        if df.empty:
            raise ValueError("Downloaded file is empty.")

        # Use row 6 (index 5) as headers
        df.columns = df.iloc[5]
        df = df[6:].reset_index(drop=True)

        # Clean column names
        df.columns = (
            df.columns
            .str.strip()
            .str.replace("\n", "", regex=True)
            .str.replace(" +", " ", regex=True)
        )

        # Drop completely empty rows
        df = df.dropna(how="all")

        required_cols = [
            "Časový interval",
            "Zobchodované množství(MWh)",
            "Zobchodované množství - nákup(MWh)",
            "Zobchodované množství - prodej(MWh)",
            "Vážený průměr cen (EUR/MWh)",
            "Minimální cena(EUR/MWh)",
            "Maximální cena(EUR/MWh)",
            "Poslední cena(EUR/MWh)",
        ]
        for c in required_cols:
            if c not in df.columns:
                raise ValueError(f"Missing column '{c}' in DataFrame.")

        df["Časový interval"] = df["Časový interval"].astype(str).str.strip()

        return df

    except Exception as e:
        print(f"Error while fetching/processing data: {e}")
        sys.exit(1)

def row_is_empty(row):
    num_cols = [
        "Zobchodované množství(MWh)",
        "Zobchodované množství - nákup(MWh)",
        "Zobchodované množství - prodej(MWh)",
        "Vážený průměr cen (EUR/MWh)",
        "Minimální cena(EUR/MWh)",
        "Maximální cena(EUR/MWh)",
        "Poslední cena(EUR/MWh)",
    ]
    for c in num_cols:
        val = row.get(c)
        if pd.notna(val) and str(val).strip() != "":
            return False
    return True

def get_fallback_row(df, start_idx):
    """Walk backward from start_idx until we find a non-empty row."""
    for i in range(start_idx, -1, -1):
        row = df.iloc[i]
        if not row_is_empty(row):
            msg = (f"No new data available after interval {row['Časový interval']}. "
                   f"Showing last known data from {row['Časový interval']}.")
            return row, msg

    if len(df) > 0:
        return df.iloc[0], "No non-empty row found; showing the earliest row."
    else:
        return None, "No data in DataFrame at all."

def get_current_time_block(df):
    """
    Find the interval covering current CET time. If not found, use the
    last interval that started before now. If row is empty, fallback
    to the last non-empty row.
    """
    cet_tz = pytz.timezone("Europe/Prague")
    now = datetime.now(cet_tz).time()

    valid_rows = []
    for idx, row in df.iterrows():
        interval_str = row["Časový interval"]
        if "Perioda" in interval_str or "Časový interval" in interval_str:
            continue

        try:
            start_str, end_str = interval_str.split("-")
            start_str, end_str = start_str.strip(), end_str.strip()
            st = datetime.strptime(start_str, "%H:%M").time()
            et = datetime.strptime(end_str, "%H:%M").time()
        except ValueError:
            continue

        crosses_midnight = (st > et)
        valid_rows.append((idx, st, et, crosses_midnight))

    if not valid_rows:
        if len(df) > 0:
            return df.iloc[-1], "No parseable intervals found; showing last row by default."
        else:
            return None, "No data at all."

    matching_idx = None
    # Instead of storing just `last_before_idx`, store (idx, st) so we can compare `datetime.time` to `datetime.time`
    last_before = None

    for (idx, st, et, crosses_midnight) in valid_rows:
        # Check if "now" is within st..et
        if crosses_midnight:
            # e.g., 23:45-00:00
            if (now >= st) or (now < et):
                matching_idx = idx
                break
        else:
            if st <= now < et:
                matching_idx = idx
                break

        # Track the last interval that started before now
        if st <= now:
            if last_before is None:
                last_before = (idx, st)
            else:
                # Compare st to the previously stored time
                if st > last_before[1]:
                    last_before = (idx, st)

    # If we found an exact match
    if matching_idx is not None:
        row = df.iloc[matching_idx]
        if row_is_empty(row):
            return get_fallback_row(df, matching_idx)
        else:
            return row, ""

    # No exact match, fallback to "last_before" or earliest
    if last_before is not None:
        row = df.iloc[last_before[0]]
        if row_is_empty(row):
            return get_fallback_row(df, last_before[0])
        else:
            msg = (f"No exact match for current time. "
                   f"Showing last known data from {row['Časový interval']}.")
            return row, msg
    else:
        # All intervals start after now
        first_idx = valid_rows[0][0]
        row = df.iloc[first_idx]
        if row_is_empty(row):
            return get_fallback_row(df, first_idx)
        msg = (f"All intervals start after {now.strftime('%H:%M')}. "
               f"Showing earliest interval in data: {row['Časový interval']}.")
        return row, msg

def generate_html(row, fallback_message, output_file="index.html"):
    cet_tz = pytz.timezone("Europe/Prague")
    now_cet = datetime.now(cet_tz)
    now_str = now_cet.strftime("%Y-%m-%d %H:%M:%S")

    # Round up to next quarter hour
    next_run_cet = next_quarter_hour(now_cet)
    next_run_str = next_run_cet.strftime("%Y-%m-%d %H:%M:%S")

    if row is None:
        html_content = f"""<!DOCTYPE html>
<html lang="cs">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Electricity Market Data</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f9f9f9;
        }}
        .warning {{
            color: red;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <h1>Electricity Market Data Viewer</h1>
    <p><strong>Last Updated (CET):</strong> {now_str}</p>
    <p class="warning">{fallback_message}</p>
    <p><em>Next scheduled update (approx.): {next_run_str} (CET)</em></p>
</body>
</html>"""
    else:
        interval = row.get("Časový interval", "NA")
        zm = row.get("Zobchodované množství(MWh)", "NA")
        zmn = row.get("Zobchodované množství - nákup(MWh)", "NA")
        zmp = row.get("Zobchodované množství - prodej(MWh)", "NA")
        vp = row.get("Vážený průměr cen (EUR/MWh)", "NA")
        minc = row.get("Minimální cena(EUR/MWh)", "NA")
        maxc = row.get("Maximální cena(EUR/MWh)", "NA")
        pc = row.get("Poslední cena(EUR/MWh)", "NA")

        html_content = f"""<!DOCTYPE html>
<html lang="cs">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Electricity Market Data</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f9f9f9;
        }}
        h1 {{
            color: #333;
        }}
        p {{
            font-size: 16px;
            color: #555;
        }}
        .warning {{
            color: red;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <h1>Electricity Market Data Viewer</h1>
    <p><strong>Last Updated (CET):</strong> {now_str}</p>
    <p>Ci: {interval} 
       ZM{zm} 
       ZMN{zmn} 
       ZMp{zmp} 
       VP{vp} 
       MinC{minc} 
       MaxC{maxc} 
       PC{pc}
    </p>
    <p><em>Next scheduled update (approx.): {next_run_str} (CET)</em></p>
    {"<p class='warning'>" + fallback_message + "</p>" if fallback_message else ""}
    <!-- Script run at {datetime.utcnow()} UTC -->
</body>
</html>
"""

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"HTML file '{output_file}' has been generated.")

def main():
    print("Fetching data...")
    df = fetch_and_process_data()

    print("Selecting time block...")
    row, fallback_msg = get_current_time_block(df)

    print("Generating HTML...")
    generate_html(row, fallback_msg)
    print("Done.")

if __name__ == "__main__":
    main()
