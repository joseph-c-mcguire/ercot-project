import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("ercot_tracking.log"), logging.StreamHandler()],
)

# Configure retries
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

# Define the base URL for the ERCOT API
base_url = "https://www.ercot.com/gridinfo/load/load_hist"

# Calculate the date range for the last 60 days
end_date = datetime.now()
start_date = end_date - timedelta(days=60)

# Format the dates as strings
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Define headers
headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# Define the parameters for the API request
params = {
    "id": "historical_data",
    "reportType": "load",
    "startDate": start_date_str,
    "endDate": end_date_str,
}

# Make the API request with error handling
try:
    logging.info(
        f"Requesting data from {base_url} for period {start_date_str} to {end_date_str}"
    )
    response = session.get(base_url, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    # Parse the JSON response
    data = response.json()
    logging.info(f"Retrieved data successfully")

    # Convert the data to a pandas DataFrame
    # Adjust the DataFrame creation based on the actual response structure
    if isinstance(data, dict) and "data" in data:
        df = pd.DataFrame(data["data"])
    else:
        df = pd.DataFrame(data)

    # Save the DataFrame to a CSV file
    output_file = f"ercot_load_data_{start_date_str}_to_{end_date_str}.csv"
    df.to_csv(output_file, index=False)
    logging.info(f"Data successfully saved to {output_file}")

except requests.exceptions.RequestException as e:
    logging.error(f"Request failed: {str(e)}")
    if hasattr(e, "response") and e.response is not None:
        logging.error(f"Status code: {e.response.status_code}")
        logging.error(f"Response headers: {e.response.headers}")
        logging.error(f"Response content: {e.response.text[:500]}")  # First 500 chars
except json.JSONDecodeError as e:
    logging.error(f"Failed to parse JSON response: {str(e)}")
except Exception as e:
    logging.error(f"Unexpected error: {str(e)}")
