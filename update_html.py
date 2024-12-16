import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import BytesIO
from datetime import datetime
import os
import pytz

# Function to fetch and process the Excel file from the website
def fetch_and_process_data():
    try:
        # URL of the data page
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

        # Build file link and fetch Excel
        file_href = link_tag["href"]
        file_link = "https://www.ote-cr.cz" + file_href
        file_response = requests.get(file_link, timeout=10)
        file_response.raise_for_status()

        # Read Excel file into DataFrame
        excel_file = BytesIO(file_response.content)
        df = pd.read_excel(excel_file, header=None)
        if df.empty:
            raise ValueError("Downloaded file is empty.")

        # Process the DataFrame
        df.columns = df.iloc[5]  # Use the 6th row as headers
        df = df[6:].reset_index(drop=True)

        # Clean column names
        df.columns = (
            df.columns.str.strip()
            .str.replace("\n", "", regex=True)
            .str.replace(" +", " ", regex=True)
        )

        # Drop empty rows
        df = df.dropna(how="all")
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None



def get_current_time_block(df):
    """
    Find the latest available time block relative to the current CET time.
    """
    # Get current CET time
    cet_timezone = pytz.timezone("Europe/Prague")
    current_time = datetime.now(cet_timezone)
    formatted_time = current_time.strftime("%H:%M")

    # Safely convert 'Časový interval' column to strings and filter out invalid rows
    df["Časový interval"] = df["Časový interval"].astype(str).str.strip()
    valid_times = df["Časový interval"].dropna()
    
    # Convert intervals to datetime for comparison
    time_blocks = []
    for interval in valid_times:
        try:
            # Extract time (handles formats like '20:30-20:45')
            start_time, _ = interval.split("-")
            time_blocks.append((interval, datetime.strptime(start_time, "%H:%M")))
        except ValueError:
            continue  # Skip invalid time formats

    # Sort time blocks by start time
    time_blocks.sort(key=lambda x: x[1])

    # Find the latest time block before the current time
    previous_time_block = None
    for interval, start_time in time_blocks:
        if start_time.time() <= current_time.time():
            previous_time_block = interval
        else:
            break

    # Retrieve the row with the latest time block
    if previous_time_block:
        return df[df["Časový interval"] == previous_time_block].iloc[0]
    
    return None




# Function to generate the HTML file
def generate_html(row, output_file="index.html"):
    # Get current CET time
    cet_timezone = pytz.timezone("Europe/Prague")
    current_time_cet = datetime.now(cet_timezone).strftime("%Y-%m-%d %H:%M:%S")

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
    <p><strong>Last Updated (CET):</strong> {current_time_cet}</p>
    <p>Ci: {row['Časový interval']} 
       ZM{row['Zobchodované množství(MWh)']} 
       ZMN{row['Zobchodované množství - nákup(MWh)']} 
       ZMp{row['Zobchodované množství - prodej(MWh)']} 
       VP{row['Vážený průměr cen (EUR/MWh)']} 
       MinC{row['Minimální cena(EUR/MWh)']} 
       MaxC{row['Maximální cena(EUR/MWh)']} 
       PC{row['Poslední cena(EUR/MWh)']}
    </p>
</body>
</html>"""
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"HTML file '{output_file}' has been generated.")



# Main execution
def main():
    print("Fetching data...")
    df = fetch_and_process_data()
    if df is not None:
        print("Processing data...")
        current_row = get_current_time_block(df)
        if current_row is not None:
            print("Generating HTML...")
            generate_html(current_row)
        else:
            print("No matching time block found for the current time.")
    else:
        print("Failed to fetch data.")


if __name__ == "__main__":
    main()
