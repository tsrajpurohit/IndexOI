name: Run NSE Option Chain Script

on:
  push:
    branches:
      - main

jobs:
  run-script:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt

      - name: Set up Google credentials
        run: |
          echo "$GOOGLE_CREDENTIALS_JSON" > credentials.json
          export GOOGLE_SHEETS_CREDENTIALS=$PWD/credentials.json
        env:
          GOOGLE_CREDENTIALS_JSON: ${{ secrets.GOOGLE_SHEETS_CREDENTIALS }}

      - name: Run the Python script
        run: |
          python nseoptionchain.py
