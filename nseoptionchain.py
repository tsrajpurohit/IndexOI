import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from nsepython import *
import os
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the scope for Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials from environment variable
credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

if not credentials_path:
    logger.error("Google API credentials path not found. Set the 'GOOGLE_SHEETS_CREDENTIALS' environment variable.")
    raise ValueError("Google API credentials path not found.")

# Authorize the client
try:
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(credentials)
    logger.info("Successfully authenticated with Google Sheets API.")
except Exception as e:
    logger.error(f"Failed to authenticate with Google Sheets API: {e}")
    raise

# Create or open the Google Sheet
sheet_name = "Pankaj_Power"
try:
    sheet = client.open(sheet_name)
    logger.info(f"Opened existing Google Sheet: {sheet_name}")
except gspread.SpreadsheetNotFound:
    sheet = client.create(sheet_name)
    logger.info(f"Created new Google Sheet: {sheet_name}")

# Function to open or create a worksheet
def open_or_create_sheet(sheet, title, rows=1000, cols=20):
    try:
        worksheet = sheet.worksheet(title)
        logger.info(f"Opened worksheet: {title}")
    except gspread.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
        logger.info(f"Created new worksheet: {title}")
    return worksheet

# Function to update worksheet with DataFrame data
def update_sheet_with_df(worksheet, df):
    try:
        worksheet.clear()  # Clear the worksheet before updating
        worksheet.update([df.columns.values.tolist()] + df.fillna("").values.tolist())
        logger.info(f"Worksheet {worksheet.title} updated successfully.")
    except Exception as e:
        logger.error(f"Failed to update worksheet {worksheet.title}: {e}")
        raise

# Open the additional info worksheet
additional_info_sheet = open_or_create_sheet(sheet, "Additional Info")

# Fetch NIFTY OI data and handle any potential exceptions
try:
    oi_data_nifty, ltp_nifty, crontime_nifty = oi_chain_builder("NIFTY", "latest", "full")
    oi_data_nifty_df = pd.DataFrame(oi_data_nifty)
    nifty_sheet = open_or_create_sheet(sheet, "NIFTY OI Data")
    update_sheet_with_df(nifty_sheet, oi_data_nifty_df)
    logger.info("NIFTY OI data processed successfully.")
except Exception as e:
    logger.error(f"Error fetching or processing NIFTY data: {e}")

# Fetch BANKNIFTY OI data and handle any potential exceptions
try:
    oi_data_banknifty, ltp_banknifty, crontime_banknifty = oi_chain_builder("BANKNIFTY", "latest", "full")
    oi_data_banknifty_df = pd.DataFrame(oi_data_banknifty)
    banknifty_sheet = open_or_create_sheet(sheet, "BANKNIFTY OI Data")
    update_sheet_with_df(banknifty_sheet, oi_data_banknifty_df)
    logger.info("BANKNIFTY OI data processed successfully.")
except Exception as e:
    logger.error(f"Error fetching or processing BANKNIFTY data: {e}")

# Clear and update additional info like LTP and CronTime for both NIFTY and BANKNIFTY
try:
    additional_info_sheet.clear()
    additional_info_sheet.update([
        ["Index", "LTP", "CronTime"],
        ["NIFTY", ltp_nifty, crontime_nifty],
        ["BANKNIFTY", ltp_banknifty, crontime_banknifty]
    ])
    logger.info("Additional info (LTP and CronTime) updated successfully.")
except Exception as e:
    logger.error(f"Error updating additional info: {e}")

print("Data saved to Google Sheets successfully!")
