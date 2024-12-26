import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta, time
import pytz
import sys
import os

# ----------------------------------------------------------------------------------------
# Helper function to round up to the next quarter hour
# ----------------------------------------------------------------------------------------
def next_quarter_hour(now):
    """
    Return a datetime rounded up to the next quarter hour: xx:00, xx:15, xx:30, xx:45.
    If now is exactly on a quarter (00, 15, 30, 45) and second=0, we keep that time.
    Otherwise, we go to the next quarter hour.
    Also handles rolling over hour/day if needed.
    """
    # Convert minutes to "which quarter"  (0..3)
    quarter = now.minute // 15

    # If the current time is already on a perfect quarter boundary
    if now.minute % 15 == 0 and now.second == 0:
        next_quarter = quarter
    else:
        next_quarter = quarter + 1

    # The new minute mark
    new_minute = next_quarter * 15
    new_hour = now.hour
    new_day = now.day

    if new_minute == 60:
        # We rolled over to the next hour
        new_minute = 0
        new_hour += 1
        if new_hour == 24:
            # If needed, handle day rollover
            new_hour = 0
            new_day += 1
            # This example won't handle month/year rollover, but you can add that if needed.

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
        sys.exit(1)  # so that GitHub Actions marks the job as failed if there's an error


# ----------------------------------------------------------------------------------------
# Find the row corresponding to the current CET time block
# ----------------------------------------------------------------------------------------
def get_current_time_block(df):
    """
    Attempt to find the row whose interval includes the current CET time.
    If not found, pick the latest time block that started before 'now'.
    """

    # Current CET time
    cet_timezone = pytz.timezone("Europe/Prague")
    now = datetime.now(cet_timezone).time()  # e.g. 11:27:52

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
            continue  # skip malformed intervals

        crosses_midnight = start_t > end_t
        valid_rows.append((idx, start_t, end_t, crosses_midnight))

    if not valid_rows:
        # If no rows were parseable, fallback to the last row in df
        print("No parseable intervals found; returning last row by default.")
        return df.iloc[-1]

    matching_idx = None
    last_before_now_idx = None

    for (idx, start_t, end_t, crosses_midnight) in valid_rows:
        if crosses_midnight:
            # e.g., 23:45-00:00. "now" is in interval if now >= start_t or now < end_t
            if (now >= start_t) or (now < end_t):
                matching_idx = idx
        else:
            # Normal interval, e.g. 10:30-10:45
            if start_t <= now < end_t:
                matching_idx = idx

        # Update "latest that started before now"
        if start_t <= now:
            if (last_before_now_idx is None) or (start_t > valid_rows[last_before_now_idx][1]):
                last_before_now_idx = valid_rows.index((idx, start_t, end_t, crosses_midnight))

        if matching_idx is not None:
            break

    if matching_idx is not None:
        return df.iloc[matching_idx]
    else:
        if last_before_now_idx is not None:
            chosen_idx = valid_rows[last_before_now_idx][0]
            return df.iloc[chosen_idx]
        else:
            # All intervals start after now, pick the first interval
            print("All intervals start after current time; picking first interval in the day.")
            return df.iloc[valid_rows[0][0]]


# ----------------------------------------------------------------------------------------
# Generate the HTML file
# ----------------------------------------------------------------------------------------
def generate_html(row, output_file="index.html"):
    # Current CET time
    cet_timezone = pytz.timezone("Europe/Prague")
    current_time_cet = datetime.now(cet_timezone)
    current_time_str = current_time_cet.strftime("%Y-%m-%d %H:%M:%S")

    # Instead of "current_time + 15 minutes", we do "round up to next quarter hour"
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
    current_row = get_current_time_block(df)
    print("Generating HTML...")
    generate_html(current_row)
    print("Done.")


if __name__ == "__main__":
    main()
