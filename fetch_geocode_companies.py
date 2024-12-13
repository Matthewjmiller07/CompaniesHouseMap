import pandas as pd
import requests
import logging
import time
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    filename="geocode_companies.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# GeoNames API setup
GEONAMES_USERNAME = "matthewjmiller07"
GEONAMES_POSTAL_URL = "http://api.geonames.org/postalCodeSearchJSON"

def geocode_postal_code(postal_code):
    """
    Geocode a postal code using the GeoNames Postal Code API.
    """
    params = {
        "postalcode": postal_code,
        "country": "GB",  # Country code for the United Kingdom
        "maxRows": 1,  # Fetch only the top result
        "username": GEONAMES_USERNAME,  # GeoNames username
    }
    try:
        response = requests.get(GEONAMES_POSTAL_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if "postalCodes" in data and data["postalCodes"]:
            lat = data["postalCodes"][0]["lat"]
            lng = data["postalCodes"][0]["lng"]
            logging.info(f"Geocoded postal code '{postal_code}' to ({lat}, {lng}).")
            return lat, lng
        else:
            logging.warning(f"No geocode results for postal code: {postal_code}.")
            return None, None
    except Exception as e:
        logging.error(f"Error geocoding postal code '{postal_code}': {e}")
        return None, None

def process_csv_with_progress(input_file, output_file):
    """
    Process the CSV file to geocode postal codes with a progress bar and save updates sequentially.
    """
    try:
        df = pd.read_csv(input_file)
        if "RegAddress.PostCode" not in df.columns:
            logging.error("Column 'RegAddress.PostCode' not found in the CSV.")
            return

        # Add Latitude and Longitude columns if they don't exist
        if "Latitude" not in df.columns:
            df["Latitude"] = None
        if "Longitude" not in df.columns:
            df["Longitude"] = None

        # Use tqdm for a live progress bar
        with tqdm(total=len(df), desc="Geocoding Postal Codes", unit="row") as pbar:
            for index, row in df.iterrows():
                if pd.isna(row["Latitude"]) or pd.isna(row["Longitude"]):  # Skip already geocoded
                    postal_code = row["RegAddress.PostCode"]
                    if pd.notna(postal_code):
                        lat, lng = geocode_postal_code(postal_code)
                        df.at[index, "Latitude"] = lat
                        df.at[index, "Longitude"] = lng
                        time.sleep(1)  # To respect GeoNames rate limits
                    
                    # Save progress incrementally
                    df.iloc[:index + 1].to_csv(output_file, index=False)

                pbar.update(1)  # Update progress bar

        logging.info(f"Geocoding complete. Data saved to {output_file}.")
    except Exception as e:
        logging.critical(f"Critical error while processing CSV: {e}")

# Main execution
if __name__ == "__main__":
    input_file = "/Applications/Apps/CompaniesHouseMap/nw4_companies_filtered.csv"
    output_file = "/Applications/Apps/CompaniesHouseMap/nw4_geocoded_companies_with_sic.csv"
    process_csv_with_progress(input_file, output_file)