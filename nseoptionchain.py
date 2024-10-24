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

# Get the absolute path of the current directory and specify the credentials file
current_directory = os.path.dirname(os.path.abspath(__file__))  # Get the current directory
credentials_path = os.path.join(current_directory, "credentials.json")  # Combine with the filename
logging.info(f"Using credentials from: {credentials_path}")

# Function to authorize Google Sheets client
def authorize_google_sheets(credentials_path, scope):
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        return gspread.authorize(credentials)
    except Exception as e:
        logging.error(f"Failed to load credentials: {e}")
        raise

# Function to create or open a Google Sheet
def create_or_open_sheet(client, sheet_name):
    try:
        return client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        logging.info(f"Creating a new Google Sheet: {sheet_name}")
        return client.create(sheet_name)

# Function to create or get a worksheet
def get_or_create_worksheet(sheet, title, rows="1000", cols="20"):
    try:
        return sheet.worksheet(title)
    except gspread.WorksheetNotFound:
        logging.info(f"Adding worksheet: {title}")
        return sheet.add_worksheet(title=title, rows=rows, cols=cols)

# Function to fetch OI data and update the corresponding Google Sheet
def fetch_and_update_oi_data(sheet, index_name):
    try:
        oi_data, ltp, crontime = oi_chain_builder(index_name, "latest", "full")
        oi_data_df = pd.DataFrame(oi_data)
        
        # Update the respective worksheet
        sheet_name = f"{index_name} OI Data"
        worksheet = get_or_create_worksheet(sheet, sheet_name)
        
        # Clear existing data and upload new data
        worksheet.clear()
        worksheet.update([oi_data_df.columns.values.tolist()] + oi_data_df.fillna("").values.tolist())
        
        logging.info(f"{index_name} OI Data fetched successfully.")
        logging.info(f"LTP {index_name}: {ltp}")
        logging.info(f"CronTime {index_name}: {crontime}")
        
        return ltp, crontime
    except Exception as e:
        logging.error(f"Failed to fetch and update OI data for {index_name}: {e}")
        return None, None

# Main execution flow
if __name__ == "__main__":
    # Authorize the client
    client = authorize_google_sheets(credentials_path, scope)

    # Create or open the main Google Sheet
    sheet = create_or_open_sheet(client, "Pankaj_Power")

    # Open or create the "Additional Info" worksheet
    additional_info_sheet = get_or_create_worksheet(sheet, "Additional Info")

    # Fetch and update OI data for NIFTY
    ltp_nifty, crontime_nifty = fetch_and_update_oi_data(sheet, "NIFTY")

    # Fetch and update OI data for BANKNIFTY
    ltp_banknifty, crontime_banknifty = fetch_and_update_oi_data(sheet, "BANKNIFTY")

    # Clear and update additional info
    additional_info_sheet.clear()
    additional_info_sheet.update([
        ["Index", "LTP", "CronTime"],
        ["NIFTY", ltp_nifty, crontime_nifty],
        ["BANKNIFTY", ltp_banknifty, crontime_banknifty]
    ])

    logging.info("Data saved to Google Sheets successfully!")
