import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from nsepython import *
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the scope for Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials from the JSON file specified in the environment variable
credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "credentials.json")
logging.info(f"Using credentials from: {credentials_path}")

try:
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
except Exception as e:
    logging.error(f"Failed to load credentials: {e}")
    raise

# Authorize the client
client = gspread.authorize(credentials)

# Create or open the Google Sheet
try:
    sheet = client.open("Pankaj_Power")
except gspread.SpreadsheetNotFound:
    logging.info("Creating a new Google Sheet: Pankaj_Power")
    sheet = client.create("Pankaj_Power")

# Open the first sheet (worksheet) for additional info
try:
    additional_info_sheet = sheet.worksheet("Additional Info")
except gspread.WorksheetNotFound:
    logging.info("Adding worksheet: Additional Info")
    additional_info_sheet = sheet.add_worksheet(title="Additional Info", rows="1000", cols="20")

# Get Open Interest (OI) data from NSE for NIFTY
oi_data_nifty, ltp_nifty, crontime_nifty = oi_chain_builder("NIFTY", "latest", "full")
oi_data_nifty_df = pd.DataFrame(oi_data_nifty)

# Add or open a new sheet for NIFTY OI data
try:
    nifty_sheet = sheet.worksheet("NIFTY OI Data")
except gspread.WorksheetNotFound:
    logging.info("Adding worksheet: NIFTY OI Data")
    nifty_sheet = sheet.add_worksheet(title="NIFTY OI Data", rows="1000", cols="20")

# Upload NIFTY OI data
nifty_sheet.clear()
nifty_sheet.update([oi_data_nifty_df.columns.values.tolist()] + oi_data_nifty_df.fillna("").values.tolist())

# Print NIFTY OI data, LTP, and CronTime
logging.info("NIFTY OI Data fetched successfully.")
logging.info(f"LTP NIFTY: {ltp_nifty}")
logging.info(f"CronTime NIFTY: {crontime_nifty}")

# Get Open Interest (OI) data from NSE for BANKNIFTY
oi_data_banknifty, ltp_banknifty, crontime_banknifty = oi_chain_builder("BANKNIFTY", "latest", "full")
oi_data_banknifty_df = pd.DataFrame(oi_data_banknifty)

# Add or open a new sheet for BANKNIFTY OI data
try:
    banknifty_sheet = sheet.worksheet("BANKNIFTY OI Data")
except gspread.WorksheetNotFound:
    logging.info("Adding worksheet: BANKNIFTY OI Data")
    banknifty_sheet = sheet.add_worksheet(title="BANKNIFTY OI Data", rows="1000", cols="20")

# Upload BANKNIFTY OI data
banknifty_sheet.clear()
banknifty_sheet.update([oi_data_banknifty_df.columns.values.tolist()] + oi_data_banknifty_df.fillna("").values.tolist())

# Print BANKNIFTY OI data, LTP, and CronTime
logging.info("BANKNIFTY OI Data fetched successfully.")
logging.info(f"LTP BANKNIFTY: {ltp_banknifty}")
logging.info(f"CronTime BANKNIFTY: {crontime_banknifty}")

# Clear and update additional info
additional_info_sheet.clear()
additional_info_sheet.update([
    ["Index", "LTP", "CronTime"],
    ["NIFTY", ltp_nifty, crontime_nifty],
    ["BANKNIFTY", ltp_banknifty, crontime_banknifty]
])

logging.info("Data saved to Google Sheets successfully!")
