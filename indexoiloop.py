import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from nsepython import *
import json
import os

# Define the scope for Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials from the environment variable
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

if not credentials_json:
    raise ValueError("No credentials found. Please set the GOOGLE_SHEETS_CREDENTIALS environment variable.")

try:
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(credentials_json), scope)
except json.JSONDecodeError as e:
    raise ValueError(f"Failed to parse JSON from GOOGLE_SHEET_CREDENTIALS: {e}")

# Authorize the client
client = gspread.authorize(credentials)

# Create or open the Google Sheet
try:
    sheet = client.open("Pankaj_Power")
except gspread.SpreadsheetNotFound:
    sheet = client.create("Pankaj_Power")

# Open or create additional info sheet
try:
    additional_info_sheet = sheet.worksheet("Additional Info")
except gspread.WorksheetNotFound:
    additional_info_sheet = sheet.add_worksheet(title="Additional Info", rows="1000", cols="20")

def fetch_and_update_data():
    # Get Open Interest (OI) data for NIFTY
    oi_data_nifty, ltp_nifty, crontime_nifty = oi_chain_builder("NIFTY", "latest", "full")
    oi_data_nifty_df = pd.DataFrame(oi_data_nifty)
    try:
        nifty_sheet = sheet.worksheet("NIFTY OI Data")
    except gspread.WorksheetNotFound:
        nifty_sheet = sheet.add_worksheet(title="NIFTY OI Data", rows="1000", cols="20")
    nifty_sheet.clear()
    nifty_sheet.update([oi_data_nifty_df.columns.values.tolist()] + oi_data_nifty_df.fillna("").values.tolist())

    # Get OI data for BANKNIFTY
    oi_data_banknifty, ltp_banknifty, crontime_banknifty = oi_chain_builder("BANKNIFTY", "latest", "full")
    oi_data_banknifty_df = pd.DataFrame(oi_data_banknifty)
    try:
        banknifty_sheet = sheet.worksheet("BANKNIFTY OI Data")
    except gspread.WorksheetNotFound:
        banknifty_sheet = sheet.add_worksheet(title="BANKNIFTY OI Data", rows="1000", cols="20")
    banknifty_sheet.clear()
    banknifty_sheet.update([oi_data_banknifty_df.columns.values.tolist()] + oi_data_banknifty_df.fillna("").values.tolist())

    # Get OI data for FINNIFTY
    oi_data_finnifty, ltp_finnifty, crontime_finnifty = oi_chain_builder("FINNIFTY", "latest", "full")
    oi_data_finnifty_df = pd.DataFrame(oi_data_finnifty)
    try:
        finnifty_sheet = sheet.worksheet("FINNIFTY OI Data")
    except gspread.WorksheetNotFound:
        finnifty_sheet = sheet.add_worksheet(title="FINNIFTY OI Data", rows="1000", cols="20")
    finnifty_sheet.clear()
    finnifty_sheet.update([oi_data_finnifty_df.columns.values.tolist()] + oi_data_finnifty_df.fillna("").values.tolist())

    # Update additional info with LTP and CronTime
    additional_info_sheet.clear()
    additional_info_sheet.update([
        ["Index", "LTP", "CronTime"],
        ["NIFTY", ltp_nifty, crontime_nifty],
        ["BANKNIFTY", ltp_banknifty, crontime_banknifty],
        ["FINNIFTY", ltp_finnifty, crontime_finnifty]
    ])

    print("Data saved to Google Sheets successfully!")

# Run the fetch-and-update process every 3 minutes from 9:15 AM to 3:30 PM
start_time = datetime.strptime("09:15", "%H:%M").time()
end_time = datetime.strptime("15:30", "%H:%M").time()

print("Starting the process...")

while True:
    current_time = datetime.now().time()
    if start_time <= current_time <= end_time:
        try:
            print(f"Fetching data at {datetime.now().strftime('%H:%M:%S')}...")
            fetch_and_update_data()
        except Exception as e:
            print(f"Error during fetch: {e}")
        time.sleep(180)  # Wait for 3 minutes
    elif current_time > end_time:
        print("Trading hours over. Stopping the process.")
        break
    else:
        time_to_start = datetime.combine(datetime.today(), start_time) - datetime.now()
        print(f"Waiting for market to open. Starts in {time_to_start.seconds // 60} minutes.")
        time.sleep(60)  # Check every minute if it's past 9:15
