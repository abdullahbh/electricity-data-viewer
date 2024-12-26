import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta, time
import pytz
import sys
import os

# ----------------------------------------------------------------------------------------
# Helper function to round up to the next quarter hour (unchanged from prior solution)
# ----------------------------------------------------------------------------------------
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


# ----------------------------------------------------------------------------------------
# Fetch and process data from the OTE website
# ----------------------------------------------------------------------------------------
def fetch_and_process_data():
    try:
        url = "https://www.ote-cr.cz/cs/kratkodobe-trhy/elektrina/vnitrodenni-trh"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Parse the HTML to get the Excel file link
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

        # Read Excel file into DataFrame
        excel_file = BytesIO(file_response.content)
        df = pd.read_excel(excel_file, header=None)
        if df.empty:
            raise ValueError("Downloaded file is empty.")

        # Use row 6 (index 5) as headers
        df.columns = df.iloc[5]
        df = df[6:].reset_index(drop=True)

        # Clean column names
        df.columns = (
            df.columns.str.strip()
            .str.replace("\n", "", regex=True)
            .str.replace(" +", " ", regex=True)
        )

        # Drop empty rows
        df = df.dropna(how="all")

        # Verify we have the columns we need
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
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"Missing expected column '{col}' in DataFrame.")

        # Convert "Časový interval" to string and strip
        df["Časový interval"] = df["Časový interval"].astype(str).str.strip()

        return df

    except Exception as e:
        print(f"Error while fetching/processing data: {e}")
        sys.exit(1)


# ----------------------------------------------------------------------------------------
# Check if a row is effectively "empty" of data
# ----------------------------------------------------------------------------------------
def row_is_empty(row, required_cols):
    """
    Return True if all required columns are NaN or blank.
    (We skip the 'Časový interval' column because that might still have a time.)
    """
    for col in required_cols:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() != "":
            return False
    return True


# ----------------------------------------------------------------------------------------
# Find the row corresponding to the current CET time block (or the last known data)
# ----------------------------------------------------------------------------------------
def get_current_time_block(df):
    """
    Attempt to find the row whose interval includes the current CET time.
    If that row is empty, or if no matching interval is found, fallback to 
    the last known (non-empty) row. 
    Also return a fallback_message if we used older data.
    """

    # We'll track if we've had to fallback
    fallback_message = ""

    # Current CET time
    cet_timezone = pytz.timezone("Europe/Prague")
    now = datetime.now(cet_timezone).time()  # e.g., 11:27:52

    # Identify valid rows with parseable intervals
    valid_rows = []
    for idx, row in df.iterrows():
        interval_str = row["Časový interval"]
        if pd.isna(interval_str):
            continue
        try:
            start_str, end_str = interval_str.split("-")
            start_str, end_str = start_str.strip(), end_str.strip()
            start_t = datetime.strptime(start_str, "%H:%M").time()
            end_t = datetime.strptime(end_str, "%H:%M").time()
        except ValueError:
            continue

        crosses_midnight = start_t > end_t
        valid_rows.append((idx, start_t, end_t, crosses_midnight))

    if not valid_rows:
        print("No parseable intervals found; returning last row by default.")
        return df.iloc[-1], "No parseable intervals in the data."

    matching_idx = None
    last_before_now_idx = None

    for (idx, start_t, end_t, crosses_midnight) in valid_rows:
        if crosses_midnight:
            # E.g. 23:45-00:00 -> now is in interval if now >= start_t or now < end_t
            if (now >= start_t) or (now < end_t):
                matching_idx = idx
        else:
            if start_t <= now < end_t:
                matching_idx = idx

        # Track the latest interval that started before now
        if start_t <= now:
            if (last_before_now_idx is None) or (start_t > valid_rows[last_before_now_idx][1]):
                last_before_now_idx = valid_rows.index((idx, start_t, end_t, crosses_midnight))

        if matching_idx is not None:
            break

    if matching_idx is not None:
        # We found a row for the "current" time block
        row = df.iloc[matching_idx]
        # Check if it actually has data
        required_data_cols = [
            "Zobchodované množství(MWh)",
            "Zobchodované množství - nákup(MWh)",
            "Zobchodované množství - prodej(MWh)",
            "Vážený průměr cen (EUR/MWh)",
            "Minimální cena(EUR/MWh)",
            "Maximální cena(EUR/MWh)",
            "Poslední cena(EUR/MWh)",
        ]
        if row_is_empty(row, required_data_cols):
            # Fallback to last known data
            row, fallback_message = get_last_non_empty_row(df, matching_idx)
        else:
            return row, fallback_message
        return row, fallback_message
    else:
        # No exact match, fallback to last_before_now or earliest
        if last_before_now_idx is not None:
            chosen_idx = valid_rows[last_before_now_idx][0]
            row = df.iloc[chosen_idx]
            required_data_cols = [
                "Zobchodované množství(MWh)",
                "Zobchodované množství - nákup(MWh)",
                "Zobchodované množství - prodej(MWh)",
                "Vážený průměr cen (EUR/MWh)",
                "Minimální cena(EUR/MWh)",
                "Maximální cena(EUR/MWh)",
                "Poslední cena(EUR/MWh)",
            ]
            if row_is_empty(row, required_data_cols):
                # Even that row is empty, fallback further
                row, fallback_message = get_last_non_empty_row(df, chosen_idx)
            else:
                fallback_message = f"No exact match for current time; showing last known data from {row['Časový interval']}."
            return row, fallback_message
        else:
            # All intervals start after now, pick the first interval in the day
            fallback_message = "All intervals are in the future; showing earliest interval in the data."
            row = df.iloc[valid_rows[0][0]]
            return row, fallback_message


def get_last_non_empty_row(df, current_idx):
    """
    Look backward from current_idx until we find a row with real data.
    Return that row + a fallback_message.
    """
    required_data_cols = [
        "Zobchodované množství(MWh)",
        "Zobchodované množství - nákup(MWh)",
        "Zobchodované množství - prodej(MWh)",
        "Vážený průměr cen (EUR/MWh)",
        "Minimální cena(EUR/MWh)",
        "Maximální cena(EUR/MWh)",
        "Poslední cena(EUR/MWh)",
    ]
    for idx in range(current_idx, -1, -1):
        row = df.iloc[idx]
        if not row_is_empty(row, required_data_cols):
            msg = (f"No new data available after interval {row['Časový interval']}. "
                   f"Showing last known data from {row['Časový interval']}.")
            return row, msg

    # If we reach here, no non-empty row found, return the first row + generic message
    first_row = df.iloc[0]
    return first_row, "No non-empty row found at all; showing earliest row from table."


# ----------------------------------------------------------------------------------------
# Generate the HTML file
# ----------------------------------------------------------------------------------------
def generate_html(row, fallback_message, output_file="index.html"):
    # Current CET time
    cet_timezone = pytz.timezone("Europe/Prague")
    current_time_cet = datetime.now(cet_timezone)
    current_time_str = current_time_cet.strftime("%Y-%m-%d %H:%M:%S")

    # Round up to the next quarter hour
    next_run_cet = next_quarter_hour(current_time_cet)
    next_run_str = next_run_cet.strftime("%Y-%m-%d %H:%M:%S")

    # Safely get row values or use "NA" if not found
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
    <p><strong>Last Updated (CET):</strong> {current_time_str}</p>

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
    
    <!-- Always update this comment so there's a file diff -->
    <!-- Script run at {datetime.utcnow()} UTC -->
</body>
</html>
"""

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"HTML file '{output_file}' has been generated.")


# ----------------------------------------------------------------------------------------
# Main execution
# ----------------------------------------------------------------------------------------
def main():
    print("Fetching data...")
    df = fetch_and_process_data()
    print("Selecting the current time block from the data...")
    row, fallback_message = get_current_time_block(df)
    print("Generating HTML...")
    generate_html(row, fallback_message)
    print("Done.")


if __name__ == "__main__":
    main()
