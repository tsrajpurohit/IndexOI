import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from nsepython import *
import os
import json

# Define the scope for Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials from environment variable
# You should store your Google API credentials file path in an environment variable
credentials_path = "credentials.json"  # Fixed to use the correct file path


if not credentials_path:
    raise ValueError("Google API credentials path not found. Set the 'GOOGLE_SHEETS_CREDENTIALS' environment variable.")

credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)

# Authorize the client
client = gspread.authorize(credentials)

# Create or open the Google Sheet
sheet_name = "Pankaj_Power"
try:
    # Open an existing Google Sheet
    sheet = client.open(sheet_name)
except gspread.SpreadsheetNotFound:
    # If the sheet doesn't exist, create a new one
    sheet = client.create(sheet_name)

# Function to open or create a worksheet
def open_or_create_sheet(sheet, title, rows=1000, cols=20):
    try:
        worksheet = sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    return worksheet

# Function to update worksheet with DataFrame data
def update_sheet_with_df(worksheet, df):
    worksheet.clear()  # Clear the worksheet before updating
    worksheet.update([df.columns.values.tolist()] + df.fillna("").values.tolist())

# Open the additional info worksheet
additional_info_sheet = open_or_create_sheet(sheet, "Additional Info")

# Get Open Interest (OI) data from NSE for NIFTY
oi_data_nifty, ltp_nifty, crontime_nifty = oi_chain_builder("NIFTY", "latest", "full")

# Convert NIFTY OI data into a DataFrame
oi_data_nifty_df = pd.DataFrame(oi_data_nifty)

# Open or create a new sheet for NIFTY OI data
nifty_sheet = open_or_create_sheet(sheet, "NIFTY OI Data")

# Upload NIFTY OI data to its dedicated sheet
update_sheet_with_df(nifty_sheet, oi_data_nifty_df)

# Print NIFTY OI data, LTP, and CronTime
print("NIFTY OI Data:")
print(oi_data_nifty)
print("LTP NIFTY:", ltp_nifty)
print("CronTime NIFTY:", crontime_nifty)

# Get Open Interest (OI) data from NSE for BANKNIFTY
oi_data_banknifty, ltp_banknifty, crontime_banknifty = oi_chain_builder("BANKNIFTY", "latest", "full")

# Convert BANKNIFTY OI data into a DataFrame
oi_data_banknifty_df = pd.DataFrame(oi_data_banknifty)

# Open or create a new sheet for BANKNIFTY OI data
banknifty_sheet = open_or_create_sheet(sheet, "BANKNIFTY OI Data")

# Upload BANKNIFTY OI data to its dedicated sheet
update_sheet_with_df(banknifty_sheet, oi_data_banknifty_df)

# Print BANKNIFTY OI data, LTP, and CronTime
print("BANKNIFTY OI Data:")
print(oi_data_banknifty)
print("LTP BANKNIFTY:", ltp_banknifty)
print("CronTime BANKNIFTY:", crontime_banknifty)

# Clear and update additional info like LTP and CronTime for both NIFTY and BANKNIFTY
additional_info_sheet.clear()
additional_info_sheet.update([
    ["Index", "LTP", "CronTime"],
    ["NIFTY", ltp_nifty, crontime_nifty],
    ["BANKNIFTY", ltp_banknifty, crontime_banknifty]
])

print("Data saved to Google Sheets successfully!")
