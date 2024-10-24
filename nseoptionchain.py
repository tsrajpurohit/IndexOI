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

# Open the first sheet (worksheet) for additional info
try:
    additional_info_sheet = sheet.worksheet("Additional Info")
except gspread.WorksheetNotFound:
    additional_info_sheet = sheet.add_worksheet(title="Additional Info", rows="1000", cols="20")

# Get Open Interest (OI) data from NSE for NIFTY
oi_data_nifty, ltp_nifty, crontime_nifty = oi_chain_builder("NIFTY", "latest", "full")

# Convert NIFTY OI data into a DataFrame
oi_data_nifty_df = pd.DataFrame(oi_data_nifty)

# Add or open a new sheet for NIFTY OI data
try:
    nifty_sheet = sheet.worksheet("NIFTY OI Data")
except gspread.WorksheetNotFound:
    nifty_sheet = sheet.add_worksheet(title="NIFTY OI Data", rows="1000", cols="20")

# Upload NIFTY OI data to its dedicated sheet
nifty_sheet.clear()  # Clear the worksheet before updating
nifty_sheet.update([oi_data_nifty_df.columns.values.tolist()] + oi_data_nifty_df.fillna("").values.tolist())

# Print NIFTY OI data, LTP, and CronTime
print("NIFTY OI Data:")
print(oi_data_nifty)
print("LTP NIFTY:", ltp_nifty)
print("CronTime NIFTY:", crontime_nifty)

# Get Open Interest (OI) data from NSE for BANKNIFTY
oi_data_banknifty, ltp_banknifty, crontime_banknifty = oi_chain_builder("BANKNIFTY", "latest", "full")

# Convert BANKNIFTY OI data into a DataFrame
oi_data_banknifty_df = pd.DataFrame(oi_data_banknifty)

# Add or open a new sheet for BANKNIFTY OI data
try:
    banknifty_sheet = sheet.worksheet("BANKNIFTY OI Data")
except gspread.WorksheetNotFound:
    banknifty_sheet = sheet.add_worksheet(title="BANKNIFTY OI Data", rows="1000", cols="20")

# Upload BANKNIFTY OI data to its dedicated sheet
banknifty_sheet.clear()  # Clear the worksheet before updating
banknifty_sheet.update([oi_data_banknifty_df.columns.values.tolist()] + oi_data_banknifty_df.fillna("").values.tolist())

# Print BANKNIFTY OI data, LTP, and CronTime
print("BANKNIFTY OI Data:")
print(oi_data_banknifty)
print("LTP BANKNIFTY:", ltp_banknifty)
print("CronTime BANKNIFTY:", crontime_banknifty)

# Clear and update additional info like LTP and CronTime for both NIFTY and BANKNIFTY
additional_info_sheet.clear()
additional_info_sheet.update([["Index", "LTP", "CronTime"], ["NIFTY", ltp_nifty, crontime_nifty], ["BANKNIFTY", ltp_banknifty, crontime_banknifty]])

print("Data saved to Google Sheets successfully!")
